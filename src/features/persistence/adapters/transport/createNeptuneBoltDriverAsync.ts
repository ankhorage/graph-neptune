import { Sha256 } from '@aws-crypto/sha256-js';
import { defaultProvider } from '@aws-sdk/credential-provider-node';
import { HttpRequest } from '@smithy/protocol-http';
import { SignatureV4 } from '@smithy/signature-v4';
import neo4j, { type Driver } from 'neo4j-driver';

import type { NeptuneGraphPersistenceOptions } from '../../../../types/neptune.js';
import type { parseNeptuneEndpoint } from './parseNeptuneEndpoint.js';

/*** Open one TLS Bolt driver with a newly signed Neptune IAM credential. */
export const createNeptuneBoltDriverAsync = async (
  options: NeptuneGraphPersistenceOptions,
  endpoint: ReturnType<typeof parseNeptuneEndpoint>,
): Promise<Driver> => {
  const auth =
    options.iamAuthentication === false
      ? neo4j.auth.basic('username', '')
      : await createIamAuthAsync(options, endpoint);
  return neo4j.driver(`bolt://${endpoint.host}:${endpoint.port}`, auth, {
    encrypted: 'ENCRYPTION_ON',
    trust: 'TRUST_SYSTEM_CA_SIGNED_CERTIFICATES',
    maxConnectionPoolSize: 1,
    ...(options.connectionTimeoutMs === undefined
      ? {}
      : { connectionTimeout: options.connectionTimeoutMs }),
  });
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
