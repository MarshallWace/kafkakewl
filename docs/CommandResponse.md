# CommandResponse

It's the response wrapper for any request (command) that has succeeded. Note that even when a request succeeds, some kafka actions may still fail.

It can contain exactly one of the following keys (depending on the request sent):

**`kafkaCluster`**: [`EntityState[KafkaCluster]`](EntityState.md) - an entity state that contains a [kafka-cluster](kafkacluster/KafkaCluster.md)

**`kafkaClusters`**: [`Array[EntityState[KafkaCluster]]`](EntityState.md) an array of entity states that contains [kafka-clusters](kafkacluster/KafkaCluster.md)

**`permission`**: [`EntityState[Permission]`](EntityState.md) - an entity state that contains a [permission](permission/Permission.md)

**`permissions`**: [`Array[EntityState[Permission]]`](EntityState.md) an array of entity states that contains [permission](permission/Permission.md)

**`topology`**: [`EntityState[Topology]`](EntityState.md) - an entity state that contains a [topology](topology/Topology.md)

**`topologies`**: [`Array[EntityState[Topology]]|Map[String, ResolvedDeployedTopology]`](EntityState.md) an array of entity states that contains [topologies](topology/Topology.md) OR a map of [resolved-topologies](resolveddeployedtopology/ResolvedDeployedTopology.md) by their topology id

Note that currently none of the end-points returns `Map[String, ResolvedDeployedTopology]`.

**`deployment`**: [`EntityState[Deployment]`](EntityState.md) - an entity state that contains a [deployment](deployment/Deployment.md)

**`deployments`**: [`Array[EntityState[Deployment]]`](EntityState.md) an array of entity states that contains [deployments](deployment/Deployment.md)

**`deployedtopology`**: [`EntityState[DeployedTopology]`](EntityState.md) - an entity state that contains a [deployed-topology](deployedtopology/DeployedTopology.md)

**`deployedtopologies`**: [`Array[EntityState[DeployedTopology]] | Map[String, ResolvedDeployedTopology]`](EntityState.md) an array of entity states that contains [deployed-topologies](deployedtopology/DeployedTopology.md) OR a map of [resolved-topologies](resolveddeployedtopology/ResolvedDeployedTopology.md) by their topology id

It depends on the request which one you get.

**`deployedTopologyMetrics`**: `DeployedTopologyMetrics` - a [deployed-topology-metrics](deployedtopologymetrics/DeployedTopologyMetrics.md)

**`deployedTopologiesMetrics`**: `Array[DeployedTopologyMetrics]` an array of [deployed-topology-metrics](deployedtopologymetrics/DeployedTopologyMetrics.md)

**`TopicIds`**: `Array[String]` - an array of local topic identifiers

**`ApplicationIds`**: `Array[String]` - an array of local application identifiers

**`DeployedTopic`**: `DeployedTopic` - a [deployed topic](deployedtopology/DeployedTopic.md)

**`DeployedApplication`**: `DeployedApplication` - a [deployed application](deployedtopology/DeployedApplication.md)

**`ApplicationOffsetsReset`**: `ApplicationOffsetsReset` - the [application offsets that were reset](deployedtopology/ApplicationOffsetsReset.md)

**`DiffResult`**: `DiffResult` - a kafka [diff-result](admin/DiffResult.md)
