# Deployment / DeploymentOptions

**`topicsToRecreate`**: [`Array[FlexibleTopicId]?`](../FlexibleTopicId.md) - an optional array of [FlexibleTopicIds](../FlexibleTopicId.md) specifying which topics must be deleted and re-created. By default it's empty.

**`topicsToRecreateResetGroups`**: `Boolean?` - if set to `true` (**default**), before re-creating the specified topics by **`topicsToRecreate`** it resets the consumer group for those topics to the beginning

**`allowedUnsafeChanges`**: [`Array[AllowedUnsafeChange]?`](AllowedUnsafeChange.md) - an optional array of [AllowedUnsafeChanges](AllowedUnsafeChange.md) to specify what unsafe changes are allowed to perform

**`authorizationCode`**: `String?` - the optional authorization code (for some topologies / kafka-clusters it's mandatory to confirm the deployment by entering the authorization code that kafkakewl generated)

# Examples

To delete and create again all topics whose id ends with `sink`:
```json
{
  "topicsToRecreate": [{ "regex": ".*sink" }]
}
```

To specify an authorization code for prod deployments:
```json
{
  "authorizationCode": "{auth-code}"
}
```
