# Deployment

This entity is sent from the server to the client, never the other way around. The client sends the [DeploymentChange](DeploymentChange.md) entity, when it wants to create or update a deployment.

**`kafkaClusterId`**: `String` - the kafka-cluster id

**`topologyId`**: `String` - the topology id

**`topologyVersion`**: `Object` - specifies the version of the topology that is expected to be deployed to the kafka-cluster.

It can have the following values:
- `{"remove": true}`: the topology should be removed from (shouldn't be deployed to) the kafka-cluster
- `{"latestTracking": true}`: the topology's latest version must be deployed to the kafka-cluster, automatically, whenever there is a new version
- `{"exact": [exact-topology-version-number]}`: the topology's specified version must be deployed to the kafka-cluster

**`tags`**: `Array[Strings]?` - an optional array of string tags for the deployment. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the deployment. By default it's empty and it's completely ignored by kafkakewl.
