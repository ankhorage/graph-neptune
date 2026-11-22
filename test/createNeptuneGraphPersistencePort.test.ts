import { beforeEach, expect, mock, test } from 'bun:test';

interface Query {
  readonly statement: string;
  readonly parameters: Readonly<Record<string, unknown>>;
}
interface BoltRecordFixture {
  readonly get: (key: string) => unknown;
}
interface BoltResultFixture {
  readonly records: readonly BoltRecordFixture[];
}
interface TransactionFixture {
  readonly queries: Query[];
  readonly commit: ReturnType<typeof mock>;
  readonly rollback: ReturnType<typeof mock>;
  readonly run: ReturnType<typeof mock>;
}
interface DriverFixture {
  readonly driver: {
    readonly close: ReturnType<typeof mock>;
    readonly session: ReturnType<typeof mock>;
  };
  readonly read: TransactionFixture;
  readonly write: TransactionFixture;
}

const drivers: DriverFixture[] = [];
const authTokens: { readonly principal: string; readonly credentials: string }[] = [];
let signatureCount = 0;

mock.module('neo4j-driver', () => ({
  default: {
    auth: {
      basic: (principal: string, credentials: string) => {
        authTokens.push({ principal, credentials });
        return { principal, credentials };
      },
    },
    driver: () => {
      const fixture = drivers.shift();
      if (fixture === undefined) throw new Error('Missing mocked Bolt driver.');
      return fixture.driver;
    },
    isInt: (value: unknown) =>
      typeof value === 'object' && value !== null && 'inSafeRange' in value && 'toNumber' in value,
    isNode: (value: unknown) =>
      typeof value === 'object' && value !== null && 'labels' in value && 'properties' in value,
    isRelationship: (value: unknown) =>
      typeof value === 'object' && value !== null && 'type' in value && 'properties' in value,
    session: { READ: 'READ', WRITE: 'WRITE' },
  },
}));

mock.module('@aws-sdk/credential-provider-node', () => ({
  defaultProvider: () => async () => ({ accessKeyId: 'test-key', secretAccessKey: 'test-secret' }),
}));

mock.module('@smithy/protocol-http', () => ({
  HttpRequest: class {
    readonly headers: Record<string, string>;
    readonly method: string;

    constructor(input: { readonly headers: Record<string, string>; readonly method: string }) {
      this.headers = input.headers;
      this.method = input.method;
    }
  },
}));

mock.module('@smithy/signature-v4', () => ({
  SignatureV4: class {
    async sign(request: { readonly headers: Record<string, string>; readonly method: string }) {
      signatureCount += 1;
      return {
        ...request,
        headers: {
          ...request.headers,
          authorization: `credential-${signatureCount}`,
          'x-amz-date': '20261122T000000Z',
        },
      };
    }
  },
}));

mock.module('@aws-crypto/sha256-js', () => ({ Sha256: class {} }));

const { createNeptuneGraphPersistencePort } =
  await import('../src/features/persistence/composition/createNeptuneGraphPersistencePort.js');

const result = (...records: BoltRecordFixture[]): BoltResultFixture => ({ records });
const record = (values: Readonly<Record<string, unknown>>): BoltRecordFixture => ({
  get: (key) => values[key],
});
const node = (labels: readonly string[], properties: Readonly<Record<string, unknown>>) => ({
  labels,
  properties,
});
const relationship = (type: string, properties: Readonly<Record<string, unknown>>) => ({
  type,
  properties,
});

const transaction = (responses: (BoltResultFixture | Error)[]): TransactionFixture => {
  const queries: Query[] = [];
  return {
    queries,
    commit: mock(async () => undefined),
    rollback: mock(async () => undefined),
    run: mock(async (statement: string, parameters: Readonly<Record<string, unknown>>) => {
      queries.push({ statement, parameters });
      const response = responses.shift() ?? result();
      if (response instanceof Error) throw response;
      return response;
    }),
  };
};

const driver = (
  writeResponses: (BoltResultFixture | Error)[] = [],
  readResponses: (BoltResultFixture | Error)[] = [],
): DriverFixture => {
  const write = transaction(writeResponses);
  const read = transaction(readResponses);
  const close = mock(async () => undefined);
  return {
    write,
    read,
    driver: {
      close,
      session: mock(({ defaultAccessMode }: { readonly defaultAccessMode: string }) => ({
        beginTransaction: () => write,
        close: mock(async () => undefined),
        executeRead: async (work: (value: TransactionFixture) => Promise<unknown>) =>
          await work(defaultAccessMode === 'READ' ? read : write),
      })),
    },
  };
};

const port = (iamAuthentication = true) =>
  createNeptuneGraphPersistencePort({
    endpoint: 'bolt://neptune.example.test:8182/',
    region: 'eu-central-1',
    iamAuthentication,
  });

const graphNode = (overrides: Readonly<Record<string, unknown>> = {}) => ({
  id: 'node-1',
  labels: ['Current'] as const,
  data: { properties: { name: 'Node' }, document: { revision: 1 } },
  ...overrides,
});

beforeEach(() => {
  drivers.length = 0;
  authTokens.length = 0;
  signatureCount = 0;
});

test('stable-edge-identity preserves immutable native IDs and rejects duplicate identities', async () => {
  const unchanged = driver([
    result(
      record({
        source: 'node-1',
        target: 'node-2',
        type: 'LINKS',
        id: 'edge-1',
        r: relationship('LINKS', { weight: 1 }),
      }),
    ),
  ]);
  drivers.push(unchanged);
  const session = await port().openAsync();
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        {
          kind: 'upsertEdge',
          edge: {
            id: 'edge-1',
            source: 'node-1',
            target: 'node-2',
            type: 'LINKS',
            data: { properties: { weight: 1 } },
          },
        },
      ],
    }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 0 });
  expect(unchanged.write.queries[0]?.statement).toContain('r.`~id` = $id');

  const duplicate = driver([
    result(
      record({ source: 'node-1', target: 'node-2', type: 'LINKS', r: relationship('LINKS', {}) }),
      record({ source: 'node-1', target: 'node-2', type: 'LINKS', r: relationship('LINKS', {}) }),
    ),
  ]);
  drivers.push(duplicate);
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        {
          kind: 'upsertEdge',
          edge: {
            id: 'edge-1',
            source: 'node-1',
            target: 'node-2',
            type: 'LINKS',
            data: { properties: {} },
          },
        },
      ],
    }),
  ).rejects.toThrow('Neptune operation failed.');
  expect(duplicate.write.rollback).toHaveBeenCalledTimes(1);

  const parallel = driver([
    result(),
    result(record({ r: relationship('LINKS', {}) })),
    result(),
    result(record({ r: relationship('LINKS', {}) })),
  ]);
  drivers.push(parallel);
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        {
          kind: 'upsertEdge',
          edge: {
            id: 'edge-history-1',
            source: 'node-1',
            target: 'node-2',
            type: 'LINKS',
            data: { properties: { period: 1 } },
          },
        },
        {
          kind: 'upsertEdge',
          edge: {
            id: 'edge-history-2',
            source: 'node-1',
            target: 'node-2',
            type: 'LINKS',
            data: { properties: { period: 2 } },
          },
        },
      ],
    }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 2 });
  expect(parallel.write.queries.map((query) => query.parameters.id)).toEqual([
    'edge-history-1',
    'edge-history-1',
    'edge-history-2',
    'edge-history-2',
  ]);
});

test('round-trips native scalars and structured documents through the versioned portable codec', async () => {
  const encodedDocument = '{"document":{"nested":[1,true,null]},"version":1}';
  const stored = driver([result(), result()]);
  const read = driver(
    [],
    [
      result(
        record({
          n: node(['Current'], {
            __ankh_document: encodedDocument,
            active: true,
            count: 2,
            name: 'Node',
          }),
          id: 'node-1',
        }),
      ),
    ],
  );
  drivers.push(stored, read);
  const session = await port().openAsync();
  await session.executeAsync({
    kind: 'mutate',
    mutations: [
      {
        kind: 'upsertNode',
        node: graphNode({
          data: {
            properties: { active: true, count: 2, name: 'Node' },
            document: { nested: [1, true, null] },
          },
        }),
      },
    ],
  });
  expect(stored.write.queries[1]?.statement).toContain('`~id`: $id');
  expect(stored.write.queries[1]?.parameters.properties).toEqual({
    __ankh_document: encodedDocument,
    active: true,
    count: 2,
    name: 'Node',
  });
  await expect(session.executeAsync({ kind: 'nodes', nodeIds: ['node-1'] })).resolves.toEqual({
    kind: 'nodes',
    nodes: [
      {
        id: 'node-1',
        labels: ['Current'],
        data: {
          properties: { active: true, count: 2, name: 'Node' },
          document: { nested: [1, true, null] },
        },
      },
    ],
  });
});

test('contains provider and unsupported-document errors at the adapter boundary', async () => {
  const providerFailure = driver([], [new Error('Authorization=secret-token')]);
  const unsupportedDocument = driver(
    [],
    [
      result(
        record({
          n: node(['Current'], { __ankh_document: '{"document":{},"version":2}' }),
          id: 'node-1',
        }),
      ),
    ],
  );
  drivers.push(providerFailure, unsupportedDocument);
  const session = await port().openAsync();
  await expect(session.executeAsync({ kind: 'nodes', nodeIds: ['node-1'] })).rejects.toThrow(
    'Neptune operation failed.',
  );
  await expect(session.executeAsync({ kind: 'nodes', nodeIds: ['node-1'] })).rejects.toThrow(
    'Neptune operation failed.',
  );
});

test('atomic-rollback rolls back an ordered batch when an edge endpoint is absent', async () => {
  const fixture = driver([result(), result(), result()]);
  drivers.push(fixture);
  const session = await port().openAsync();
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        { kind: 'upsertNode', node: graphNode() },
        {
          kind: 'upsertEdge',
          edge: {
            id: 'edge-1',
            source: 'node-1',
            target: 'missing',
            type: 'LINKS',
            data: { properties: {} },
          },
        },
      ],
    }),
  ).rejects.toThrow('Neptune operation failed.');
  expect(fixture.write.commit).not.toHaveBeenCalled();
  expect(fixture.write.rollback).toHaveBeenCalledTimes(1);
});

test('node-delete-safety permits isolated and ordered edge-then-node deletes but rejects attached nodes', async () => {
  const isolated = driver([result(record({ count: 0 })), result(record({ count: 1 }))]);
  const attached = driver([result(record({ count: 1 }))]);
  const ordered = driver([
    result(record({ count: 1 })),
    result(record({ count: 0 })),
    result(record({ count: 1 })),
  ]);
  drivers.push(isolated, attached, ordered);
  const session = await port().openAsync();
  await expect(
    session.executeAsync({ kind: 'mutate', mutations: [{ kind: 'deleteNode', nodeId: 'node-1' }] }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 1 });
  expect(isolated.write.queries[0]?.statement).toContain('OPTIONAL MATCH (n)');
  await expect(
    session.executeAsync({ kind: 'mutate', mutations: [{ kind: 'deleteNode', nodeId: 'node-1' }] }),
  ).rejects.toThrow('Neptune operation failed.');
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        { kind: 'deleteEdge', edgeId: 'edge-1' },
        { kind: 'deleteNode', nodeId: 'node-1' },
      ],
    }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 2 });
});

test('idempotent-mutation-count exactly replaces properties and labels without removing every label', async () => {
  const replaced = driver([result(record({ n: node(['Legacy'], { removed: true }) })), result()]);
  const repeated = driver([
    result(
      record({
        n: node(['Current'], {
          name: 'Node',
          __ankh_document: '{"document":{"revision":1},"version":1}',
        }),
      }),
    ),
  ]);
  const reserved = driver([]);
  drivers.push(replaced, repeated, reserved);
  const session = await port().openAsync();
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [{ kind: 'upsertNode', node: graphNode() }],
    }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 1 });
  const replacement = replaced.write.queries[1]?.statement ?? '';
  expect(replacement).toContain('SET n:`Current`');
  expect(replacement).toContain('REMOVE n:`Legacy`');
  expect(replacement.indexOf('SET n:`Current`')).toBeLessThan(
    replacement.indexOf('REMOVE n:`Legacy`'),
  );
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [{ kind: 'upsertNode', node: graphNode() }],
    }),
  ).resolves.toEqual({ kind: 'mutated', mutationCount: 0 });
  await expect(
    session.executeAsync({
      kind: 'mutate',
      mutations: [
        { kind: 'upsertNode', node: graphNode({ data: { properties: { '~id': 'attack' } } }) },
      ],
    }),
  ).rejects.toThrow('Neptune operation failed.');
  expect(reserved.write.rollback).toHaveBeenCalledTimes(1);
});

test('traversal-inclusion-and-order uses incoming direction and includes isolated requested roots', async () => {
  const fixture = driver(
    [],
    [
      result(
        record({
          relationship: relationship('LINKS', {}),
          id: 'edge-1',
          source: 'node-2',
          target: 'node-1',
        }),
        record({
          relationship: relationship('LINKS', {}),
          id: 'edge-2',
          source: 'node-3',
          target: 'node-1',
        }),
      ),
      result(
        record({ n: node(['Current'], {}), id: 'node-1' }),
        record({ n: node(['Current'], {}), id: 'node-2' }),
        record({ n: node(['Current'], {}), id: 'node-3' }),
      ),
    ],
  );
  const outgoing = driver(
    [],
    [result(), result(record({ n: node(['Current'], {}), id: 'node-1' }))],
  );
  const both = driver([], [result(), result(record({ n: node(['Current'], {}), id: 'node-1' }))]);
  const zeroDepth = driver([], [result(record({ n: node(['Current'], {}), id: 'node-1' }))]);
  drivers.push(fixture, outgoing, both, zeroDepth);
  const session = await port().openAsync();
  const traversal = await session.executeAsync({
    kind: 'traverse',
    startNodeIds: ['node-1'],
    direction: 'incoming',
    maxDepth: 1,
    pageSize: 2,
  });
  expect(traversal).toMatchObject({ kind: 'traversal', cursorConsistency: 'live' });
  if (traversal.kind !== 'traversal') throw new Error('Expected traversal result.');
  expect(traversal.edges.map((edge) => edge.id)).toEqual(['edge-1', 'edge-2']);
  expect(traversal.nodes.map((value) => value.id)).toEqual(['node-1', 'node-2', 'node-3']);
  expect(fixture.read.queries[0]?.statement).toContain('(start)<-[relationships*1..1]-(end)');
  await session.executeAsync({
    kind: 'traverse',
    startNodeIds: ['node-1'],
    direction: 'outgoing',
    maxDepth: 1,
    pageSize: 2,
  });
  await session.executeAsync({
    kind: 'traverse',
    startNodeIds: ['node-1'],
    direction: 'both',
    maxDepth: 1,
    pageSize: 2,
  });
  await expect(
    session.executeAsync({
      kind: 'traverse',
      startNodeIds: ['node-1'],
      direction: 'outgoing',
      maxDepth: 0,
      pageSize: 2,
    }),
  ).resolves.toMatchObject({ kind: 'traversal', edges: [] });
  expect(outgoing.read.queries[0]?.statement).toContain('(start)-[relationships*1..1]->(end)');
  expect(both.read.queries[0]?.statement).toContain('(start)-[relationships*1..1]-(end)');
  expect(zeroDepth.read.queries).toHaveLength(1);
});

test('cursor-consistency rejects cross-query cursors and refreshes IAM credentials for each operation', async () => {
  const first = driver(
    [],
    [
      result(
        record({
          relationship: relationship('LINKS', {}),
          id: 'edge-1',
          source: 'node-1',
          target: 'node-2',
        }),
        record({
          relationship: relationship('LINKS', {}),
          id: 'edge-2',
          source: 'node-1',
          target: 'node-3',
        }),
      ),
      result(
        record({ n: node(['Current'], {}), id: 'node-1' }),
        record({ n: node(['Current'], {}), id: 'node-2' }),
      ),
    ],
  );
  const rejected = driver();
  const iamFirst = driver([], [result(record({ n: node(['Current'], {}), id: 'node-1' }))]);
  const iamSecond = driver([], [result(record({ n: node(['Current'], {}), id: 'node-1' }))]);
  drivers.push(first, rejected, iamFirst, iamSecond);
  const session = await port().openAsync();
  const page = await session.executeAsync({
    kind: 'traverse',
    startNodeIds: ['node-1'],
    direction: 'outgoing',
    maxDepth: 1,
    pageSize: 1,
  });
  if (page.kind !== 'traversal' || page.nextCursor === undefined)
    throw new Error('Expected a cursor.');
  await expect(
    session.executeAsync({
      kind: 'traverse',
      startNodeIds: ['node-1'],
      direction: 'incoming',
      maxDepth: 1,
      pageSize: 1,
      cursor: page.nextCursor,
    }),
  ).rejects.toThrow('Neptune operation failed.');
  await session.executeAsync({ kind: 'nodes', nodeIds: ['node-1'] });
  await session.executeAsync({ kind: 'nodes', nodeIds: ['node-1'] });
  expect(signatureCount).toBe(4);
  expect(
    [first, rejected, iamFirst, iamSecond].every(
      (fixture) => fixture.driver.close.mock.calls.length === 1,
    ),
  ).toBe(true);

  const unauthenticated = driver([], [result(record({ n: node(['Current'], {}), id: 'node-1' }))]);
  drivers.push(unauthenticated);
  await (await port(false).openAsync()).executeAsync({ kind: 'nodes', nodeIds: ['node-1'] });
  expect(signatureCount).toBe(4);
  expect(authTokens.at(-1)?.credentials).toBe('');
});
