# DeployedTopology / NotAllowedUnsafeChange

**`operation`**: `String` - the operation (`Remove` or `Update`) that was not allowed to execute.

**`entityType`**: `String` - the entity type (`Topic` or `Acl`) that was not allowed to be updated or removed.

**`entityKey`**: `String` - the entity's key that was not allowed to be updated or removed.

This is a string and can have one of the following formats:
- `"topic:{topic-name}"`
- `"acl:{resourceType}/{resourcePatternType}/{resourceName}/{principal}/{host}/{operation}/{permission}"`

**`entityProperties`**: `Array[Object]` - the entity's property names (**`name`** field) and the changes (**`change`** field) that were not allowed to be updated.
