# Getting Started - adding permissions

By default, without defining any permissions nobody can do anything in kafkakewl - except super-users. You need permissions to read/update/deploy to [kafka-clusters](kafkacluster/KafkaCluster.md), to read/update/deploy [topologies](topology/Topology.md) and perform some other operations.

For all the details about the permission entity see [permissions](permission/Permission.md).

To get started, let's create the minimum set of permissions your user will need to manipulate any kafka-clusters and submit/deploy any topologies:

**Obviously in reality, permissions need to be more granular!**

## Permission for kafka-clusters

The permission to be able to manipulate any [kafka-clusters](kafkacluster/KafkaCluster.md):

```json
{
	"principal": "my-user-name",
	"resourceType": "KafkaCluster",
	"resourceName": { "any": true },
	"resourceOperations": ["Any"]
}
```

After it's saved as **permission.json**, you need to submit it:

`curl -XPOST localhost:8080/permission -H "Content-Type: application/json" --compressed --data @permission.json`

## Permission for topologies

The permission to be able to manipulate any [topologies](topology/Topology):

```json
{
	"principal": "my-user-name",
	"resourceType": "Namespace",
	"resourceName": { "any": true },
	"resourceOperations": ["Any"]
}
```

It can be submitted exactly the same way as the previous one.
