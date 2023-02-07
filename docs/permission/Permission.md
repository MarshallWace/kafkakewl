# Permission

**`principal`**: `String` - the principal (user name) of the permission

**`resourceType`**: `String` - the type of the resource the permission applies to.

It can be one of the following:
- `System`: the permission applies to the whole system (no resource-name is needed)
- `KafkaCluster`: the permission applies to one or more [kafka-cluster](../kafkacluster/KafkaCluster.md)s
- `Namespace`: the permission applies to one or more namespaces
- `Topology`: the permission applies to one or more [topologies](../topology/Topology.md)

**`resourceName`**: [`FlexibleName`](../FlexibleName.md) - the resource name's flexible-name

**`resourceOperations`**: `Array[Strings]` - the operations on the resource type / resource(s) that should be allowed for the principal to perform

The possible values depend on the resource-type:

- `System`:
  - `Read`: can read all/invalidate cached permissions via the `/admin` endpoint
  - `Any`: the above and also can manipulate permissions, can use the `/admin` endpoint
- `KafkaCluster`:
  - `Read`: can read [kafka-cluster](../kafkacluster/KafkaCluster.md)s
  - `Write`: can create/update/delete [kafka-cluster](../kafkacluster/KafkaCluster.md)s
  - `Deploy`: can deploy to the specified [kafka-cluster](../kafkacluster/KafkaCluster.md)s
  - `Any`: all above
- `Namespace`:
  - `Read`: can read the [topologies](../topology/Topology.md)s within the namespace(s)
  - `Write`: can create/update/delete [topologies](../topology/Topology.md)s within the namespace(s)
  - `Deploy`: can deploy [topologies](../topology/Topology.md)s within the namespace(s)
  - `ResetApplication`: can reset the applications in the [topologies](../topology/Topology.md)s within the namespace(s)
  - `Any`: all above
- `Topology`:
  - `Read`: can read the the [topologies](../topology/Topology.md)
  - `Write`: can create/update/delete the [topologies](../topology/Topology.md)s
  - `Deploy`: can deploy the [topologies](../topology/Topology.md)
  - `ResetApplication`: can reset the applications in the [topologies](../topology/Topology.md)
  - `Any`: all above

# Examples

Any operaton is allowed for that user in namespace `projectx`:
```json
{
	"principal": "my-user-name",
	"resourceType": "Namespace",
	"resourceName": { "namespace": "projectx" },
	"resourceOperations": ["Any"]
}
```

Only read operations are allowed for that user in namespace `projecty`:
```json
{
	"principal": "my-user-name",
	"resourceType": "Namespace",
	"resourceName": { "namespace": "projecty" },
	"resourceOperations": ["Read"]
}
```

Read and Deploy operations are allowed for that user in kafka-cluster `test`
```json
{
	"principal": "my-user-name",
	"resourceType": "KafkaCluster",
	"resourceName": { "test" },
	"resourceOperations": ["Read", "Deploy"]
}
```

Any operations are allowed for that user in kafka-cluster `test`
```json
{
	"principal": "admin-user",
	"resourceType": "KafkaCluster",
	"resourceName": { "test" },
	"resourceOperations": ["Any"]
}
```
