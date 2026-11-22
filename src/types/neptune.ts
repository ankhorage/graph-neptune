type NeptuneCredentialProvider = () => Promise<{
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly sessionToken?: string;
}>;

/** Configuration for one Amazon Neptune Database Bolt endpoint. */
export interface NeptuneGraphPersistenceOptions {
  readonly endpoint: string;
  readonly region: string;
  readonly iamAuthentication?: boolean;
  readonly credentialProvider?: NeptuneCredentialProvider;
  readonly connectionTimeoutMs?: number;
}
