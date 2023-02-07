# EntityState

**`metadata`**: `EntityStateMetadata` - the metadata of the entity

For details see below.

**`entity`**: `Object` - it can be one of the following entities:

- [`KafkaCluster`](kafkacluster/KafkaCluster.md)
- [`Permission`](permission/Permission.md)
- [`Topology`](topology/Topology.md)
- [`Deployment`](deployment/Deployment.md)
- [`DeployedTopology`](deployedtopology/DeployedTopology.md)

## EntityStateMetadata

**`id`**: `String` - the identifier of the entity

**`version`**: `Int` - the version of the entity

**`createdBy`**: `String` - the name of the user who created the entity

**`timeStampUtc`**: `String` - the last updated time of the entity as iso date-time
