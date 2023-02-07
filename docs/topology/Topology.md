# Topology

**`namespace`**: `String` - the namespace of the topology

It can empty, but the `namespace` + ["." + `topology`] expression must not be empty string (either `namespace` or `topology` must be non-empty). However, typically it's not empty, the project or owner team of the topology is there, possibly even more than one fragments, e.g. `team.project.subproject`. The namespace is used for the following:
- to generate the topology unique id (`namespace` + ["." + `topology`])
- kafkakewl expects the user to have the appropriate permission for the namespace
- all kafka names in the topology must be prefixed with this namespace (but not the topology): e.g. topic names, kafka consumer groups, kafka transactional ids.

**`topology`**: `String?` - the optional additional identifier within the namespace

If not specified, it's empty. Often there are multiple topologies within the same namespace (so that their kafka topic can have only that namespace prefix), and to differentiate among these, they need to have another id within the namespace. It's used for:
- generating the topology unique id, see above
- the topology unique id also serves as the container namespace for every other id in this topology (topic, application, alias ids)

**`description`**: `String?` - an optional description of the topology

**`environments`**: `Map[String, Map[String, Array[String]]] ?` - the variable values for the deployment environments

Sometimes we want to use different attributes in different kafka-clusters. E.g. the number of partitions in a test deployment can be just 1 but needs to be higher in a prod deployment. Or the user of an application needs to be svc-something-t in test and svc-something-p in prod. For this we can have a set of environments with variables. Those variables can be used in certain topic/application/relationship attributes and they get their actual value at deployment.

Also it's possible to associate these environments with the kafka-clusters in a particular order, so that a variable can have different values in different environments, and the kafka cluster's environment order will decide the overriding order. More details will follow...

## Example:

```json
{
  "environments": {
    "default": {
      "number-of-partitions": "1",
      "retention.ms": "180000000000"
    },
    "test": {
      "retention.ms": "180000000"
    },
    "prod": {
      "number-of-partitions": "4"
    }
  }
}

```
**`developers`**: `Array[String]?` - a list of user names, who need full-access in test deployments to all topics and read-only access in prod deployments

Developers are able to run all applications in the topology with their own user, kafka ACLs are setup for them.

**`developersAccess`**: `String?` - can be either `Full` or `TopicReadOnly`

Its default value is the variable `${developers-access}`, which is defined as `Full` in test clusters and `TopicReadOnly` in prod clusters. So if you don't specify it at all, you'll most likely get the behavior you want, but you can also specify it and override the default variable.

**`deployWithAuthorizationCode`**: `Boolean?` - indicates whether this topology requires an authorization code when deployed or not.

Its default value is the variable `${deploy-with-authorization-code}`, which is defines as `true` in the prod clusters, `false` everywhere else. It's useful to set it to `false` if you want to deploy your topology in an automated way even to prod. DO NOT set it to false, unless you have a good reason.

**`topics`**: [`Map[String, Topic]`](TopologyTopic.md) - the kafka topics in this topology

**`applications`**: [`Map[String, Application]`](TopologyApplication.md) - the applications in this topology

**`aliases`**: [`Aliases`](TopologyAliases.md) - the aliases in this topology

**`relationships`**: [`Map[String, Relationship]?`](TopologyRelationship.md) - the relationships in this topology

A key in this object is an identifier of an application or topic (local or external) and it's one of the two relationship participants. The value is a [`relationship`](TopologyRelationship.md), that contains the type(s) of the relationship and the other participants.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the topology. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the topology. By default it's empty and it's completely ignored by kafkakewl.

# Examples

A simple topology with a single topic that's produced and consumed:

```json
{
  "namespace": "projectx",
  "developers": ["my-user-name"],
  "topics": {
    "test-topic": { "name": "test-topic", "partitions": 4 }
  },
  "applications": {
    "test-producer": { "user": "test-producer-service-user" },
    "test-consumer": { "user": "test-consumer-service-user", "type": { "consumerGroup": "projectx.test-consumer" } },
  },
  "relationships": {
    "test-producer": {
      "produce": [ "test-topic" ]
    },
    "test-consumer": {
      "consume": [ "test-topic" ]
    }
  }
}

```
