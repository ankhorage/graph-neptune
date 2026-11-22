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
import { createTraversalCursor } from '../adapters/storage/createTraversalCursor.js';
import { createTraversalPattern } from '../adapters/storage/createTraversalPattern.js';
import { escapeCypherIdentifier } from '../adapters/storage/escapeCypherIdentifier.js';
import {
  encodeStorageData,
  mapBoltEdge,
  mapBoltNode,
  matchesBoltNode,
  matchesStorageData,
} from '../adapters/storage/mapBoltValues.js';
import { readBoltNumber } from '../adapters/storage/readBoltNumber.js';
import { parseNeptuneEndpoint } from '../adapters/transport/parseNeptuneEndpoint.js';
const nativeIdentifier = '`~id`';
const traversalCursor = createTraversalCursor();
/*** Create a TLS Neptune Database Bolt persistence port using native custom identities. */
export const createNeptuneGraphPersistencePort = (
  options: NeptuneGraphPersistenceOptions,
): GraphPersistencePort => ({ openAsync: async () => await openNeptuneSessionAsync(options) });
/*** Open a new driver for each port session so fresh IAM credentials are signed on renewal. */
const openNeptuneSessionAsync = async (
  options: NeptuneGraphPersistenceOptions,
): Promise<GraphPersistenceSession> => {
  try {
    const endpoint = parseNeptuneEndpoint(options.endpoint);
    const auth =
      options.iamAuthentication === false
        ? neo4j.auth.basic('username', '')
        : await createIamAuthAsync(options, endpoint);
    return createSession(
      neo4j.driver(`bolt://${endpoint.host}:${endpoint.port}`, auth, {
        encrypted: 'ENCRYPTION_ON',
        trust: 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',
        maxConnectionPoolSize: 1,
        ...(options.connectionTimeoutMs === undefined
          ? {}
          : { connectionTimeout: options.connectionTimeoutMs }),
      }),
    );
  } catch (error) {
    throw redactNeptuneError(error);
  }
};
/*** Sign the documented Neptune Database Bolt IAM request without retaining credential material. */
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
  const signed = await new SignatureV4({
    credentials: options.credentialProvider ?? defaultProvider(),
    region: options.region,
    service: 'neptune-db',
    sha256: Sha256,
  }).sign(request, { unsignableHeaders: new Set(['x-amz-content-sha256']) });
  return neo4j.auth.basic(
    'username',
    JSON.stringify({
      Authorization: signed.headers.authorization,
      HttpMethod: signed.method,
      'X-Amz-Date': signed.headers['x-amz-date'],
      Host: signed.headers.host,
      'X-Amz-Security-Token': signed.headers['x-amz-security-token'],
    }),
  );
};

/*** Bind the public Graph session lifecycle to one Bolt driver. */
const createSession = (driver: Driver): GraphPersistenceSession => ({
  capabilities: ['mutate', 'nodes', 'traverse'],
  executeAsync: async (operation) => await executeAsync(driver, operation),
  closeAsync: async () => await closeAsync(driver),
});
/*** Close the provider connection without exposing provider failures. */
const closeAsync = async (driver: Driver): Promise<void> => {
  try {
    await driver.close();
  } catch (error) {
    throw redactNeptuneError(error);
  }
};

/*** Validate and dispatch a canonical Graph persistence operation. */
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

/*** Commit every ordered mutation or roll back the whole batch. */
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
    await session.close().catch(() => undefined);
  }
};

/*** Apply one mutation while protecting ordered delete and identity invariants. */
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
    `MATCH (n)-[r]-() WHERE n.${nativeIdentifier} = $nodeId AND NOT r.${nativeIdentifier} IN $deletedEdgeIds RETURN count(r) AS count`,
    { nodeId: mutation.nodeId, deletedEdgeIds },
  );
  if (readBoltNumber(attached.records[0]?.get('count')) > 0)
    throw new Error('Cannot delete a graph node with attached edges outside this atomic batch.');
  return await deleteByIdAsync(transaction, 'node', mutation.nodeId);
};

/*** Upsert a node through Neptune's globally unique native custom ID. */
const upsertNodeAsync = async (
  transaction: Transaction,
  node: StoredGraphNode,
): Promise<boolean> => {
  const existing = await transaction.run(
    `MATCH (n) WHERE n.${nativeIdentifier} = $id RETURN n, n.${nativeIdentifier} AS id`,
    { id: node.id },
  );
  if (existing.records.length > 1)
    throw new Error('Neptune returned duplicate native node identities.');
  const [record] = existing.records;
  if (record !== undefined && matchesBoltNode(record, node)) return false;
  const labels = node.labels.map(escapeCypherIdentifier).join(':');
  if (record === undefined) {
    await transaction.run(`CREATE (n:${labels} {${nativeIdentifier}: $id}) SET n = $properties`, {
      id: node.id,
      properties: encodeStorageData(node.data),
    });
    return true;
  }
  const current: unknown = record.get('n');
  if (!neo4j.isNode(current)) throw new Error('Neptune returned an invalid graph node.');
  const removeLabels = current.labels.map(escapeCypherIdentifier).join(':');
  await transaction.run(
    `MATCH (n) WHERE n.${nativeIdentifier} = $id${removeLabels === '' ? '' : ` REMOVE n:${removeLabels}`} SET n = $properties SET n:${labels}`,
    { id: node.id, properties: encodeStorageData(node.data) },
  );
  return true;
};

/*** Upsert one parallel-safe historical edge without allowing endpoint or type rewrites. */
const upsertEdgeAsync = async (
  transaction: Transaction,
  edge: StoredGraphEdge,
): Promise<boolean> => {
  const existing = await transaction.run(
    `MATCH (source)-[r]->(target) WHERE r.${nativeIdentifier} = $id RETURN source.${nativeIdentifier} AS source, target.${nativeIdentifier} AS target, type(r) AS type, r, r.${nativeIdentifier} AS id`,
    { id: edge.id },
  );
  if (existing.records.length > 1)
    throw new Error('Neptune returned duplicate native edge identities.');
  const [record] = existing.records;
  if (record !== undefined) {
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
    await transaction.run(
      `MATCH ()-[r]->() WHERE r.${nativeIdentifier} = $id SET r = $properties`,
      { id: edge.id, properties: encodeStorageData(edge.data) },
    );
    return true;
  }
  const created = await transaction.run(
    `MATCH (source), (target) WHERE source.${nativeIdentifier} = $source AND target.${nativeIdentifier} = $target CREATE (source)-[r:${escapeCypherIdentifier(edge.type)} {${nativeIdentifier}: $id}]->(target) SET r = $properties RETURN r`,
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

/*** Delete an element identified by its native custom ID. */
const deleteByIdAsync = async (
  transaction: Transaction,
  kind: 'node' | 'relationship',
  id: string,
): Promise<boolean> => {
  const result = await transaction.run(
    `MATCH ${kind === 'node' ? '(element)' : '()-[element]-()'} WHERE element.${nativeIdentifier} = $id DELETE element RETURN count(element) AS count`,
    { id },
  );
  return readBoltNumber(result.records[0]?.get('count')) > 0;
};

/*** Execute read work with deterministic provider-error and lifecycle handling. */
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
    await session.close().catch(() => undefined);
  }
};

/*** Load graph nodes by native custom ID in stable order. */
const readNodesAsync = async (
  transaction: ManagedTransaction,
  nodeIds: readonly string[],
): Promise<readonly StoredGraphNode[]> => {
  if (nodeIds.length === 0) return [];
  const result = await transaction.run(
    `MATCH (n) WHERE n.${nativeIdentifier} IN $nodeIds RETURN n, n.${nativeIdentifier} AS id ORDER BY n.${nativeIdentifier}`,
    { nodeIds },
  );
  return result.records.map(mapBoltNode);
};

/*** Return a live, stable-ID-ordered traversal page including all returned-edge endpoints. */
const traverseAsync = async (
  driver: Driver,
  operation: Extract<GraphPersistenceOperation, { readonly kind: 'traverse' }>,
): Promise<GraphPersistenceResult> =>
  await withReadAsync(driver, async (transaction) => {
    const offset = traversalCursor.decode(operation.cursor, operation);
    if (operation.maxDepth === 0)
      return {
        kind: 'traversal',
        edges: [],
        nodes: await readNodesAsync(transaction, operation.startNodeIds),
        cursorConsistency: 'live',
      };
    const result = await transaction.run(
      `MATCH (start) WHERE start.${nativeIdentifier} IN $startNodeIds MATCH path = ${createTraversalPattern(operation.direction, operation.maxDepth)} WHERE $types IS NULL OR ALL(relationship IN relationships(path) WHERE type(relationship) IN $types) UNWIND relationships(path) AS relationship WITH DISTINCT relationship MATCH (source)-[relationship]->(target) RETURN relationship, relationship.${nativeIdentifier} AS id, source.${nativeIdentifier} AS source, target.${nativeIdentifier} AS target ORDER BY relationship.${nativeIdentifier} SKIP $offset LIMIT $limit`,
      {
        startNodeIds: operation.startNodeIds,
        types: operation.types ?? null,
        offset,
        limit: operation.pageSize + 1,
      },
    );
    const edges = result.records.slice(0, operation.pageSize).map(mapBoltEdge);
    const nodes = await readNodesAsync(transaction, [
      ...new Set(edges.flatMap((edge) => [edge.source, edge.target])),
    ]);
    return {
      kind: 'traversal',
      edges,
      nodes,
      cursorConsistency: 'live',
      ...(result.records.length > operation.pageSize
        ? { nextCursor: traversalCursor.encode(offset + operation.pageSize, operation) }
        : {}),
    };
  });

/*** Replace any provider exception with a fixed credential-safe public error. */
const redactNeptuneError = (_error: unknown): Error => new Error('Neptune operation failed.');
