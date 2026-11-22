/*** Escape a dynamic Cypher label or relationship type as an identifier. */
export const escapeCypherIdentifier = (value: string): string =>
  `\`${value.replaceAll('`', '``')}\``;
