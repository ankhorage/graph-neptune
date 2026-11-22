/*** Create the directed Cypher path pattern for a validated Graph traversal. */
export const createTraversalPattern = (
  direction: 'incoming' | 'outgoing' | 'both',
  maxDepth: number,
): string => {
  const relationships = `[relationships*1..${maxDepth}]`;
  return direction === 'incoming'
    ? `(start)<-${relationships}-(end)`
    : direction === 'outgoing'
      ? `(start)-${relationships}->(end)`
      : `(start)-${relationships}-(end)`;
};
