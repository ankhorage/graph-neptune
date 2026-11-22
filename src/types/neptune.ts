type NeptuneCredentialProvider = () => Promise<{
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly sessionToken?: string;
}>;

/***
 * Configuration for one Amazon Neptune Database Bolt endpoint. Native custom IDs require Neptune
 * Database engine 1.2.0.2 or later. Bolt protocol negotiation is provider-controlled and requires
 * an authorized live endpoint for compatibility verification.
 *
 * @see https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-opencypher-extensions.html
 * @see https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-opencypher-bolt.html
 */
export interface NeptuneGraphPersistenceOptions {
  readonly endpoint: string;
  readonly region: string;
  readonly iamAuthentication?: boolean;
  readonly credentialProvider?: NeptuneCredentialProvider;
  readonly connectionTimeoutMs?: number;
}
