/*** Validate a Neptune Database Bolt endpoint before it reaches the driver. */
export const parseNeptuneEndpoint = (
  value: string,
): { readonly host: string; readonly port: number } => {
  const url = new URL(value.includes('://') ? value : `bolt://${value}`);
  if (
    url.protocol !== 'bolt:' ||
    url.username !== '' ||
    url.password !== '' ||
    url.pathname !== '/'
  )
    throw new Error('Neptune endpoint must be a bare bolt host and port.');
  const port = Number(url.port || '8182');
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535)
    throw new Error('Neptune endpoint port is invalid.');
  return { host: url.hostname, port };
};
