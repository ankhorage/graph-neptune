# Public API

## createNeptuneGraphPersistencePort

Kind: `value`
Module: `src/features/persistence/composition/createNeptuneGraphPersistencePort.ts`
Source: `src/features/persistence/composition/createNeptuneGraphPersistencePort.ts:33:14`

Create a TLS Neptune Database Bolt persistence port using native custom identities.

### Signatures

- `(options: NeptuneGraphPersistenceOptions) => GraphPersistencePort`
  - options: `NeptuneGraphPersistenceOptions`
  - returns: `GraphPersistencePort`

## NeptuneGraphPersistenceOptions

Kind: `type`
Module: `src/types/neptune.ts`
Source: `src/types/neptune.ts:8:1`

### Members

| Name | Kind | Type | Required | Description |
| --- | --- | --- | --- | --- |
| connectionTimeoutMs | property | `number` | no |  |
| credentialProvider | property | `NeptuneCredentialProvider` | no |  |
| endpoint | property | `string` | yes |  |
| iamAuthentication | property | `boolean` | no |  |
| region | property | `string` | yes |  |
