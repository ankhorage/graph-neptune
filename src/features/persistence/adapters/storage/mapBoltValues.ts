import type {
  GraphJsonObject,
  StoredGraphEdge,
  StoredGraphNode,
} from '@ankhorage/graph/persistence';
import neo4j, { type Record as BoltRecord } from 'neo4j-driver';

const documentKey = '__ankh_document';
const documentCodecVersion = 1;
const reservedStorageKeys = new Set(['~id', documentKey]);

/*** Convert a validated Bolt node into a canonical stored graph node. */
export const mapBoltNode = (record: BoltRecord): StoredGraphNode => {
  const value: unknown = record.get('n');
  if (!neo4j.isNode(value)) throw new Error('Neptune returned an invalid graph node.');
  return {
    id: readString(record.get('id'), 'element ID'),
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
    id: readString(record.get('id'), 'element ID'),
    source: readString(record.get('source'), 'edge source'),
    target: readString(record.get('target'), 'edge target'),
    type: readString(value.type, 'edge type'),
    data: decodeData(value.properties),
  };
};

/*** Encode only lossless documents while leaving scalar graph properties native and filterable. */
export const encodeStorageData = (
  data: StoredGraphNode['data'],
): Record<string, string | number | boolean> => {
  assertStoragePropertyKeys(data.properties);
  return {
    ...data.properties,
    ...(data.document === undefined
      ? {}
      : {
          [documentKey]: stringifyCanonicalJson({
            document: data.document,
            version: documentCodecVersion,
          }),
        }),
  };
};

/*** Compare a provider property map with canonical storage data after removing provider metadata. */
export const matchesStorageData = (
  properties: Record<string, unknown>,
  data: StoredGraphNode['data'],
): boolean =>
  stringifyCanonicalJson(encodeStorageData(data)) === stringifyCanonicalJson(properties);

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
  const { [documentKey]: document, ...properties } = value;
  const scalars = Object.fromEntries(
    Object.entries(properties).map(([key, property]) => [key, readScalar(property, key)]),
  );
  return typeof document === 'string'
    ? { properties: scalars, document: parseDocument(document) }
    : { properties: scalars };
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
  if (neo4j.isInt(value) && value.inSafeRange()) return value.toNumber();
  throw new Error(`Neptune returned a non-scalar property: ${name}.`);
};
/*** Validate a stored JSON document after decoding it from Neptune. */
const parseDocument = (value: string): GraphJsonObject => {
  try {
    const parsed: unknown = JSON.parse(value);
    if (!isDocumentEnvelope(parsed)) throw new Error('Unsupported document codec.');
    return parsed.document;
  } catch {
    /* rejected below */
  }
  throw new Error('Neptune returned an invalid graph document.');
};

/*** Recognize the one portable structured-document codec accepted by this adapter. */
const isDocumentEnvelope = (
  value: unknown,
): value is { readonly document: GraphJsonObject; readonly version: typeof documentCodecVersion } =>
  value !== null &&
  typeof value === 'object' &&
  !Array.isArray(value) &&
  'version' in value &&
  'document' in value &&
  value.version === documentCodecVersion &&
  isGraphJsonObject(value.document);

/*** Verify that a decoded portable document remains a JSON object. */
const isGraphJsonObject = (value: unknown): value is GraphJsonObject =>
  value !== null && typeof value === 'object' && !Array.isArray(value);
/*** Copy driver labels only after asserting their portable string form. */
const toLabels = (labels: readonly string[]): readonly [string, ...string[]] => {
  const values = labels.map((label) => readString(label, 'node label'));
  if (values.length === 0) throw new Error('Neptune returned a node without labels.');
  const [first, ...rest] = values;
  if (first === undefined) throw new Error('Neptune returned a node without labels.');
  return [first, ...rest];
};

/*** Reject user data that could overwrite Neptune's native identity or adapter document field. */
const assertStoragePropertyKeys = (properties: Readonly<Record<string, unknown>>): void => {
  const reserved = Object.keys(properties).find((key) => reservedStorageKeys.has(key));
  if (reserved !== undefined)
    throw new Error(`Graph storage property ${reserved} is reserved by the Neptune adapter.`);
};

/*** Serialize JSON-like values with sorted object keys for stable exact-upsert comparisons. */
const stringifyCanonicalJson = (value: unknown): string => JSON.stringify(sortJson(value));

/*** Recursively sort object keys without changing arrays or scalar values. */
const sortJson = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(sortJson);
  if (value !== null && typeof value === 'object')
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, sortJson(nested)]),
    );
  return value;
};
