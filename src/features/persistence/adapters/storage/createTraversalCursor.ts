import type { GraphPersistenceOperation } from '@ankhorage/graph/persistence';

type TraversalOperation = Extract<GraphPersistenceOperation, { readonly kind: 'traverse' }>;

/*** Create query-scope-bound opaque traversal cursor operations. */
export const createTraversalCursor = (): {
  readonly encode: (offset: number, operation: TraversalOperation) => string;
  readonly decode: (cursor: string | undefined, operation: TraversalOperation) => number;
} => ({ encode, decode });

/*** Encode an offset with all inputs that define the complete traversal result scope. */
const encode = (offset: number, operation: TraversalOperation): string =>
  Buffer.from(JSON.stringify({ offset, scope: traversalScope(operation) })).toString('base64url');

/*** Decode only cursors that continue the exact same semantic query. */
const decode = (cursor: string | undefined, operation: TraversalOperation): number => {
  if (cursor === undefined) return 0;
  try {
    const value = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')) as {
      offset?: unknown;
      scope?: unknown;
    };
    return typeof value.offset === 'number' &&
      Number.isSafeInteger(value.offset) &&
      value.offset >= 0 &&
      JSON.stringify(value.scope) === JSON.stringify(traversalScope(operation))
      ? value.offset
      : failCursor();
  } catch {
    return failCursor();
  }
};

/*** Canonicalize set-like inputs before cursor scope comparison. */
const traversalScope = (operation: TraversalOperation) => ({
  startNodeIds: [...operation.startNodeIds].sort(),
  direction: operation.direction,
  maxDepth: operation.maxDepth,
  pageSize: operation.pageSize,
  types: operation.types === undefined ? undefined : [...operation.types].sort(),
});

/*** Reject malformed and cross-query continuations. */
const failCursor = (): never => {
  throw new Error('Graph traversal cursor is invalid.');
};
