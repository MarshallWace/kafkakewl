# Deployment / AllowedUnsafeChange

Some changes in the kafka-cluster are considered unsafe by kafkakewl. In general these are kind of changes which can result in data-loss or breaking an existing, running application:
- deleting a topic - obviously means data loss
- changing topic configuration so that it can lose messages earlier than before (e.g. decreasing `retention.ms`)
- removing ACLs - it can break existing, running applications

However the user can explicitly confirm that some or all of these operations are allowed to perform by specifying one ore more of this object in the deployment options.

**`operation`**: `String?` - the operation (`Remove` or `Update`) that's allowed to execute.

By default it's null which means ANY operation is allowed.

**`entityType`**: `String?` - the entity type (`Topic` or `Acl`) that's allowed to be updated or removed.

By default it's null which means ANY entity type is allowed.

**`entityKey`**: `String?` - the entity's key that's allowed to be updated or removed.

This is a string and can have one of the following formats:
- `"topic:{topic-name}"`
- `"acl:{resourceType}/{resourcePatternType}/{resourceName}/{principal}/{host}/{operation}/{permission}"`

By default it's null which means ANY entity is allowed.

**`entityPropertyName`**: `String?` - the entity's property name that's allowed to be updated.

It can be a kafka config key that's allowed to be updated.

By default it's null which means ANY entity property is allowed.

# Examples

No unsafe change is allowed (it's the **default**):
```json
{
  "allowedUnsafeChanges": []
}
```

ALL unsafe changes are allowed (the empty object is a default `AllowedUnsafeChange` which means everything is allowed):
```json
{
  "allowedUnsafeChanges": [{}]
}
```

ALL `Remove` unsafe changes are allowed:
```json
{
  "allowedUnsafeChanges": [{ "operation": "Remove" }]
}
```

ALL `Update` unsafe changes are allowed:
```json
{
  "allowedUnsafeChanges": [{ "operation": "Update" }]
}
```
