# KafkaClusterCommandResponse

It can be one of the following:

**`Deployment`** - the `deployActions` and the `deployedTopologyChanged` flag indicating whether the deployed-topology has changed. It's sent after deployment.

**`DiffResult`** - a kafka [diff-result](admin/DiffResult.md). It's sent after the admin diff-requests.

**`Acls`** - the `DiffResultAcls` kafka [diff-result](admin/DiffResult.md). It's sent after the admin diff-requests.

**`ApplicationOffsetsReset`**: `ApplicationOffsetsReset` - the [application offsets that were reset](deployedtopology/ApplicationOffsetsReset.md)

**`Connector`** - the results of the connector reset requests

**`ConnectReplicator`** - the results of the connect replicator reset requests

**`TopicIds`**: `Array[String]` - an array of local topic identifiers

**`ApplicationIds`**: `Array[String]` - an array of local application identifiers

**`DeployedTopic`**: `DeployedTopic` - a [deployed topic](deployedtopology/DeployedTopic.md)

**`DeployedApplication`**: `DeployedApplication` - a [deployed application](deployedtopology/DeployedApplication.md)

