import neo4j from 'neo4j-driver';

/*** Normalize a safe provider numeric value and reject malformed aggregate results. */
export const readBoltNumber = (value: unknown): number => {
  if (typeof value === 'number' && Number.isSafeInteger(value)) return value;
  if (neo4j.isInt(value) && value.inSafeRange()) return value.toNumber();
  throw new Error('Neptune returned an invalid numeric result.');
};
