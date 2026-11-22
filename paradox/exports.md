# Public API

## createNeptuneGraphPersistencePort

Kind: `value`
Module: `src/features/persistence/composition/createNeptuneGraphPersistencePort.ts`
Source: `src/features/persistence/composition/createNeptuneGraphPersistencePort.ts:30:14`

Create a TLS Neptune Database Bolt persistence port using native custom identities.

### Signatures

- `(options: NeptuneGraphPersistenceOptions) => GraphPersistencePort`
  - options: `NeptuneGraphPersistenceOptions`
  - returns: `GraphPersistencePort`

## NeptuneGraphPersistenceOptions

Kind: `type`
Module: `src/types/neptune.ts`
Source: `src/types/neptune.ts:15:1`

Configuration for one Amazon Neptune Database Bolt endpoint. Native custom IDs require Neptune
Database engine 1.2.0.2 or later. Bolt protocol negotiation is provider-controlled and requires
an authorized live endpoint for compatibility verification.

@see https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-opencypher-extensions.html
@see https://docs.aws.amazon.com/neptune/latest/userguide/access-graph-opencypher-bolt.html

### Members

| Name | Kind | Type | Required | Description |
| --- | --- | --- | --- | --- |
| connectionTimeoutMs | property | `number` | no |  |
| credentialProvider | property | `NeptuneCredentialProvider` | no |  |
| endpoint | property | `string` | yes |  |
| iamAuthentication | property | `boolean` | no |  |
| region | property | `string` | yes |  |
