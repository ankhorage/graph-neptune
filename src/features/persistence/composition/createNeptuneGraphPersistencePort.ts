import type {
  GraphPersistenceMutation,
  GraphPersistenceOperation,
  GraphPersistencePort,
  GraphPersistenceResult,
  GraphPersistenceSession,
  StoredGraphEdge,
  StoredGraphNode,
} from '@ankhorage/graph/persistence';
import { assertGraphPersistenceOperation } from '@ankhorage/graph/persistence';
import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';
import neo4j, { type Driver, type ManagedTransaction, type Transaction } from 'neo4j-driver';

import type { NeptuneGraphPersistenceOptions } from '../../../types/neptune.js';
import {
  encodeStorageData,
  mapBoltEdge,
  mapBoltNode,
  matchesBoltNode,
  matchesStorageData,
} from '../adapters/storage/mapBoltValues.js';
import { parseNeptuneEndpoint } from '../adapters/transport/parseNeptuneEndpoint.js';

const identifierKey = '__ankh_id';

/*** Create a Neptune Database-only Graph persistence port over encrypted Bolt 4.0-compatible sessions.
 * @security The adapter signs IAM authentication material per opened session and never exposes it in errors.
 */
export const createNeptuneGraphPersistencePort = (
  options: NeptuneGraphPersistenceOptions,
): GraphPersistencePort => ({
  openAsync: async () => await openNeptuneSessionAsync(options),
});

/*** Open a fresh signed Bolt driver so short-lived SigV4 credentials are never retained across port sessions. */
const openNeptuneSessionAsync = async (
  options: NeptuneGraphPersistenceOptions,
): Promise<GraphPersistenceSession> => {
  const endpoint = parseNeptuneEndpoint(options.endpoint);
  const auth =
    options.iamAuthentication === false
      ? neo4j.auth.basic('username', '')
      : await createIamAuthAsync(options, endpoint);
  const driver = neo4j.driver(`bolt://${endpoint.host}:${endpoint.port}`, auth, {
    encrypted: 'ENCRYPTION_ON',
    trust: 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',
    maxConnectionPoolSize: 1,
    ...(options.connectionTimeoutMs === undefined
      ? {}
      : { connectionTimeout: options.connectionTimeoutMs }),
  });
  return createSession(driver);
};

/*** Produce Neptune's documented SigV4 Bolt authentication token without retaining AWS credentials. */
const createIamAuthAsync = async (
  options: NeptuneGraphPersistenceOptions,
  endpoint: ReturnType<typeof parseNeptuneEndpoint>,
): Promise<neo4j.AuthToken> => {
  const request = new HttpRequest({
    method: 'GET',
    protocol: 'bolt:',
    hostname: endpoint.host,
    port: endpoint.port,
    path: '/opencypher',
    headers: { host: `${endpoint.host}:${endpoint.port}` },
  });
  const signer = new SignatureV4({
    credentials: options.credentialProvider ?? defaultProvider(),
    region: options.region,
    service: 'neptune-db',
    sha256: Sha256,
  });
  const signed = await signer.sign(request, {
    unsignableHeaders: new Set(['x-amz-content-sha256']),
  });
  const token = JSON.stringify({
    Authorization: signed.headers.authorization,
    HttpMethod: signed.method,
    'X-Amz-Date': signed.headers['x-amz-date'],
    Host: signed.headers.host,
    'X-Amz-Security-Token': signed.headers['x-amz-security-token'],
  });
  return neo4j.auth.basic('username', token);
};

/*** Expose only provider-neutral operations and close the owning driver with the session. */
const createSession = (driver: Driver): GraphPersistenceSession => ({
  capabilities: ['mutate', 'nodes', 'traverse'],
  executeAsync: async (operation) => await executeAsync(driver, operation),
  closeAsync: async () => await driver.close(),
});

/*** Validate and route an operation through explicit read or write Bolt sessions. */
const executeAsync = async (
  driver: Driver,
  operation: GraphPersistenceOperation,
): Promise<GraphPersistenceResult> => {
  assertGraphPersistenceOperation(operation);
  if (operation.kind === 'mutate') return await mutateAsync(driver, operation.mutations);
  if (operation.kind === 'nodes')
    return await withReadAsync(driver, async (transaction) => ({
      kind: 'nodes',
      nodes: await readNodesAsync(transaction, operation.nodeIds),
    }));
  return await traverseAsync(driver, operation);
};

/*** Commit all graph mutations in one explicit write transaction or roll back the entire batch. */
const mutateAsync = async (
  driver: Driver,
  mutations: readonly GraphPersistenceMutation[],
): Promise<{ readonly kind: 'mutated'; readonly mutationCount: number }> => {
  const session = driver.session({ defaultAccessMode: neo4j.session.WRITE });
  const transaction = session.beginTransaction();
  try {
    const deletedEdgeIds = mutations
      .filter((mutation) => mutation.kind === 'deleteEdge')
      .map((mutation) => mutation.edgeId);
    const outcomes: boolean[] = [];
    for (const mutation of mutations)
      outcomes.push(await mutateOneAsync(transaction, mutation, deletedEdgeIds));
    await transaction.commit();
    return { kind: 'mutated', mutationCount: outcomes.filter(Boolean).length };
  } catch (error) {
    await transaction.rollback().catch(() => undefined);
    throw redactNeptuneError(error);
  } finally {
    await session.close();
  }
};

/*** Apply one provider-neutral mutation without exposing raw query APIs. */
const mutateOneAsync = async (
  transaction: Transaction,
  mutation: GraphPersistenceMutation,
  deletedEdgeIds: readonly string[],
): Promise<boolean> => {
  if (mutation.kind === 'upsertNode') return await upsertNodeAsync(transaction, mutation.node);
  if (mutation.kind === 'upsertEdge') return await upsertEdgeAsync(transaction, mutation.edge);
  if (mutation.kind === 'deleteEdge')
    return await deleteByIdAsync(transaction, 'relationship', mutation.edgeId);
  const attached = await transaction.run(
    `MATCH (n {${identifierKey}: $nodeId})-[r]-() WHERE NOT r.${identifierKey} IN $deletedEdgeIds RETURN count(r) AS count`,
    { nodeId: mutation.nodeId, deletedEdgeIds },
  );
  if (asNumber(attached.records[0]?.get('count')) > 0)
    throw new Error('Cannot delete a graph node with attached edges outside this atomic batch.');
  return await deleteByIdAsync(transaction, 'node', mutation.nodeId);
};

/*** Upsert one labelled node while preserving its stable identifier and native scalar properties. */
const upsertNodeAsync = async (
  transaction: Transaction,
  node: StoredGraphNode,
): Promise<boolean> => {
  const existing = await transaction.run(`MATCH (n {${identifierKey}: $id}) RETURN n`, {
    id: node.id,
  });
  const [existingRecord] = existing.records;
  if (existingRecord !== undefined && matchesBoltNode(existingRecord, node)) return false;
  const labels = node.labels.map(escapeIdentifier).join(':');
  await transaction.run(`MERGE (n {${identifierKey}: $id}) SET n += $properties SET n:${labels}`, {
    id: node.id,
    properties: encodeStorageData(node.data),
  });
  return true;
};

/*** Upsert one directed native relationship after rejecting endpoint or type rewrites. */
const upsertEdgeAsync = async (
  transaction: Transaction,
  edge: StoredGraphEdge,
): Promise<boolean> => {
  const existing = await transaction.run(
    `MATCH (source)-[r {${identifierKey}: $id}]->(target) RETURN source.${identifierKey} AS source, target.${identifierKey} AS target, type(r) AS type, r`,
    { id: edge.id },
  );
  if (existing.records.length === 1) {
    const [record] = existing.records;
    if (record === undefined) throw new Error('Neptune returned an invalid existing graph edge.');
    if (
      record.get('source') !== edge.source ||
      record.get('target') !== edge.target ||
      record.get('type') !== edge.type
    )
      throw new Error('A graph edge cannot change endpoints or relationship type.');
    const relationship: unknown = record.get('r');
    if (!neo4j.isRelationship(relationship))
      throw new Error('Neptune returned an invalid graph relationship.');
    if (matchesStorageData(relationship.properties, edge.data)) return false;
    await transaction.run(`MATCH ()-[r {${identifierKey}: $id}]->() SET r += $properties`, {
      id: edge.id,
      properties: encodeStorageData(edge.data),
    });
    return true;
  }
  const type = escapeIdentifier(edge.type);
  const created = await transaction.run(
    `MATCH (source {${identifierKey}: $source}), (target {${identifierKey}: $target}) CREATE (source)-[r:${type} {${identifierKey}: $id}]->(target) SET r += $properties RETURN r`,
    {
      source: edge.source,
      target: edge.target,
      id: edge.id,
      properties: encodeStorageData(edge.data),
    },
  );
  if (created.records.length !== 1)
    throw new Error('Graph edge endpoints must exist before an edge can be created.');
  return true;
};

/*** Delete one graph element by stable identifier and report whether storage changed. */
const deleteByIdAsync = async (
  transaction: Transaction,
  kind: 'node' | 'relationship',
  id: string,
): Promise<boolean> => {
  const pattern =
    kind === 'node'
      ? `(element {${identifierKey}: $id})`
      : `()-[element {${identifierKey}: $id}]-()`;
  const result = await transaction.run(
    `MATCH ${pattern} DELETE element RETURN count(element) AS count`,
    { id },
  );
  return asNumber(result.records[0]?.get('count')) > 0;
};

/*** Execute read work in a short-lived read session and always release its connection. */
const withReadAsync = async <Result>(
  driver: Driver,
  work: (transaction: ManagedTransaction) => Promise<Result>,
): Promise<Result> => {
  const session = driver.session({ defaultAccessMode: neo4j.session.READ });
  try {
    return await session.executeRead(work);
  } catch (error) {
    throw redactNeptuneError(error);
  } finally {
    await session.close();
  }
};

/*** Look up nodes by stable identifier and map them back to canonical Graph values. */
const readNodesAsync = async (
  transaction: ManagedTransaction,
  nodeIds: readonly string[],
): Promise<readonly StoredGraphNode[]> => {
  if (nodeIds.length === 0) return [];
  const result = await transaction.run(
    `MATCH (n) WHERE n.${identifierKey} IN $nodeIds RETURN n ORDER BY n.${identifierKey}`,
    { nodeIds },
  );
  return result.records.map(mapBoltNode);
};

/*** Return one live, deterministic traversal page whose edge IDs define cursor order. */
const traverseAsync = async (
  driver: Driver,
  operation: Extract<GraphPersistenceOperation, { readonly kind: 'traverse' }>,
): Promise<GraphPersistenceResult> =>
  await withReadAsync(driver, async (transaction) => {
    const offset = decodeCursor(operation.cursor);
    const direction =
      operation.direction === 'outgoing' ? '->' : operation.direction === 'incoming' ? '<-' : '-';
    const depth = `${operation.maxDepth}`;
    const result = await transaction.run(
      `MATCH (start) WHERE start.${identifierKey} IN $startNodeIds MATCH path = (start)-[relationships*1..${depth}]${direction}(end) UNWIND relationships(path) AS relationship WITH DISTINCT relationship WHERE $types IS NULL OR type(relationship) IN $types MATCH (source)-[relationship]->(target) RETURN relationship, source.${identifierKey} AS source, target.${identifierKey} AS target ORDER BY relationship.${identifierKey} SKIP $offset LIMIT $limit`,
      {
        startNodeIds: operation.startNodeIds,
        types: operation.types ?? null,
        offset,
        limit: operation.pageSize + 1,
      },
    );
    const page = result.records.slice(0, operation.pageSize);
    const edges = page.map(mapBoltEdge);
    const ids = [...new Set(edges.flatMap((edge) => [edge.source, edge.target]))];
    return {
      kind: 'traversal',
      edges,
      nodes: await readNodesAsync(transaction, ids),
      cursorConsistency: 'live',
      ...(result.records.length > operation.pageSize
        ? { nextCursor: encodeCursor(offset + operation.pageSize) }
        : {}),
    };
  });

const escapeIdentifier = (value: string): string => `\`${value.replaceAll('`', '``')}\``;
const asNumber = (value: unknown): number =>
  typeof value === 'number' ? value : Number(value?.toString());
const encodeCursor = (offset: number): string =>
  Buffer.from(JSON.stringify({ offset })).toString('base64url');
const decodeCursor = (cursor: string | undefined): number => {
  if (cursor === undefined) return 0;
  try {
    const value = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as {
      offset?: unknown;
    };
    return typeof value.offset === 'number' &&
      Number.isSafeInteger(value.offset) &&
      value.offset >= 0
      ? value.offset
      : failCursor();
  } catch {
    return failCursor();
  }
};
const failCursor = (): never => {
  throw new Error('Graph traversal cursor is invalid.');
};
const redactNeptuneError = (error: unknown): Error =>
  new Error(
    `Neptune operation failed: ${error instanceof Error ? error.message.replace(/(?:Authorization|X-Amz-Security-Token|Credential)=[^,\s]+/giu, '[redacted]') : 'unknown error'}`,
  );
