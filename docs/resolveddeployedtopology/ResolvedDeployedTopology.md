# ResolvedDeployedTopology

Most of these fields means the same as in the [topology](../topology/Topology.md). The most notable exception is the relationship: these are resolved with the aliases.

**`namespace`**: `String` - the namespace of the topology

**`topology`**: `String` - the optional additional identifier within the namespace

**`description`**: `String?` - an optional description of the topology

**`developers`**: `Array[String]?` - the developers of the topology

**`developersAccess`**: `String` - the developers' access

**`topics`**: [`Map[String, Topic]`](../topology/TopologyTopic.md) - the kafka topics in this topology

**`applications`**: [`Map[String, Application]`](../topology/TopologyApplication.md) - the applications in this topology

**`relationships`**: `Array[ResolvedTopologyRelationship]` - the resolved relationships in this topology

For details see below.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the topology

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the topology

## ResolvedTopologyRelationship

**`topologyId1`**: `String` - the first node's topology's id

**`id1`**: `String` - the first node's fully qualified id

**`topologyId2`**: `String` - the second node's topology's id

**`id2`**: `String` - the second node's fully qualified id

**`relationship`**: `String` - the type of the relationship

Can be `consume`, `produce` or any other custom value.

**`properties`**: `ResolvedTopologyRelationshipProperties` - the properties of the relationship, same as the full syntax in the [topology relationship](../topology/TopologyRelationship.md)
