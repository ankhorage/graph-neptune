import type {
  GraphJsonObject,
  StoredGraphEdge,
  StoredGraphNode,
} from '@ankhorage/graph/persistence';
import neo4j, { type Record as BoltRecord } from 'neo4j-driver';

const identifierKey = '__ankh_id';
const documentKey = '__ankh_document';

/*** Convert a validated Bolt node into a canonical stored graph node. */
export const mapBoltNode = (record: BoltRecord): StoredGraphNode => {
  const value: unknown = record.get('n');
  if (!neo4j.isNode(value)) throw new Error('Neptune returned an invalid graph node.');
  return {
    id: readIdentifier(value.properties),
    labels: toLabels(value.labels),
    data: decodeData(value.properties),
  };
};

/*** Convert a validated Bolt relationship and endpoint projection into a canonical graph edge. */
export const mapBoltEdge = (record: BoltRecord): StoredGraphEdge => {
  const value: unknown = record.get('relationship');
  if (!neo4j.isRelationship(value))
    throw new Error('Neptune returned an invalid graph relationship.');
  return {
    id: readIdentifier(value.properties),
    source: readString(record.get('source'), 'edge source'),
    target: readString(record.get('target'), 'edge target'),
    type: readString(value.type, 'edge type'),
    data: decodeData(value.properties),
  };
};

/*** Encode only lossless documents while leaving scalar graph properties native and filterable. */
export const encodeStorageData = (
  data: StoredGraphNode['data'],
): Record<string, string | number | boolean> => ({
  ...data.properties,
  ...(data.document === undefined ? {} : { [documentKey]: JSON.stringify(data.document) }),
});

/*** Compare a provider property map with canonical storage data after removing provider metadata. */
export const matchesStorageData = (
  properties: Record<string, unknown>,
  data: StoredGraphNode['data'],
): boolean => JSON.stringify(encodeStorageData(data)) === JSON.stringify(properties);

/*** Compare a Bolt node with its canonical upsert form without trusting untyped driver values. */
export const matchesBoltNode = (record: BoltRecord, node: StoredGraphNode): boolean => {
  const value: unknown = record.get('n');
  return (
    neo4j.isNode(value) &&
    toLabels(value.labels).every((label) => node.labels.includes(label)) &&
    node.labels.every((label) => toLabels(value.labels).includes(label)) &&
    matchesStorageData(value.properties, node.data)
  );
};

/*** Decode provider data and prevent reserved persistence metadata from entering public properties. */
const decodeData = (value: Record<string, unknown>): StoredGraphNode['data'] => {
  const { [identifierKey]: _identifier, [documentKey]: document, ...properties } = value;
  const scalars = Object.fromEntries(
    Object.entries(properties).map(([key, property]) => [key, readScalar(property, key)]),
  );
  return typeof document === 'string'
    ? { properties: scalars, document: parseDocument(document) }
    : { properties: scalars };
};

/*** Require the stable internal identity property on every persisted element. */
const readIdentifier = (properties: Record<string, unknown>): string => {
  const { [identifierKey]: identifier } = properties;
  return readString(identifier, 'element ID');
};
/*** Narrow a provider value to a non-empty string. */
const readString = (value: unknown, name: string): string => {
  if (typeof value !== 'string' || value.trim() === '')
    throw new Error(`Neptune returned an invalid ${name}.`);
  return value;
};
/*** Restrict public properties to the scalar storage contract. */
const readScalar = (value: unknown, name: string): string | number | boolean => {
  if (
    typeof value === 'string' ||
    typeof value === 'boolean' ||
    (typeof value === 'number' && Number.isFinite(value))
  )
    return value;
  throw new Error(`Neptune returned a non-scalar property: ${name}.`);
};
/*** Validate a stored JSON document after decoding it from Neptune. */
const parseDocument = (value: string): GraphJsonObject => {
  try {
    const parsed: unknown = JSON.parse(value);
    if (parsed !== null && typeof parsed === 'object' && !Array.isArray(parsed))
      return parsed as GraphJsonObject;
  } catch {
    /* rejected below */
  }
  throw new Error('Neptune returned an invalid graph document.');
};
/*** Copy driver labels only after asserting their portable string form. */
const toLabels = (labels: readonly string[]): readonly [string, ...string[]] => {
  const values = labels.map((label) => readString(label, 'node label'));
  if (values.length === 0) throw new Error('Neptune returned a node without labels.');
  const [first, ...rest] = values;
  if (first === undefined) throw new Error('Neptune returned a node without labels.');
  return [first, ...rest];
};
