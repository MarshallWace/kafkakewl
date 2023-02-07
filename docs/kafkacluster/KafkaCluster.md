# KafkaCluster

**`kafkaCluster`**: `String` - the unique id of the kafka-cluster

**`brokers`**: `String` - the comma separated list of kafka brokers

**`securityProtocol`**: `String?` - the optional `securityProtocol` config value for kafka

**`kafkaClientConfig`**: `Map[String, String]?` - an optional object containing string key-values for additional kafka config.

```json
{
    "kafkaClientConfig": {
        "sasl.kerberos.kinit.cmd": "echo",
    }
}
```

**`name`**: `String?` - the optional human readable name of the kafka-cluster

**`environments`**: `Map[String, String]?` - the optional environments that this kafka-cluster is associated with, in their specific orders. Environments also can contain variables. By default it's an empty object.

```json
{
    "environments": {
        "default": {},
        "test": {
            "some-variable": "default-value-in-test"
        }
    }
}
```

An environment is a really just a bunch of variables with values together. For example `default` environment contains your variables' default values, `test` environment may override a few of those with the values for the test cluster(s), `prod` environment may override them differently for the prod cluster(s).

It's possible to have other environments too, but ultimately every kafka-cluster defines the set of environments that it's associated with and their order too (to define the order in which variables overrides one another)

Variables for environments can be defined in the kafka-cluster and the topology too (these have higher precedence than the ones in the kafka-cluster). Some variables are defined in code too, see below.

Variables can be used in the topologies when they are deployed to a particular kafka-cluster. E.g. a topic can have different number of partitions depending on the cluster its topology is deployed to.

This cluster is associated with the `default` and `test` environments (order is important!), and defines a variable `some-variable`. This can be used in topologies when deployed to this cluster. Topologies can also override variables.

The order of environments defines the order of variable overriding: from first to last. For example, if a topology defines `some-variable` in the `default` environment with value "topology-value", at deployment it'll still have the value of "default-value-in-test", because test overrides default. However it the topology defines `some-variable` in the `test` environment, it'll have the value of "topology-value".

Variable override works the same way even when there are no pre-defined variables in the kafka-cluster (typically there aren't any).

There are a few variables which are defined in code for certain environments:
```json
{
  "default": {
    "developers-access": "TopicReadOnly",
    "deploy-with-authorization-code": "false"
  },
  "test": {
    "env": "test",
    "short-env": "t",
    "developers-access": "Full",
    "deploy-with-authorization-code": "false"
  },
  "staging": {
    "env": "staging",
    "short-env": "s",
    "developers-access": "TopicReadOnly",
    "deploy-with-authorization-code": "false"
  },
  "prod": {
    "env": "prod",
    "short-env": "p",
    "developers-access": "TopicReadOnly",
    "deploy-with-authorization-code": "true"
  },
}
```

It may be a good idea to use these environment names, in your kafka-clusters, but ultimately it's not mandatory. If you want, you can use only a few of these, or none of them even.

**`nonKewl`**: [`Array[NonKewlKafkaResources]?`](NonKewlKafkaResources.md) - the resources (topics and acls) in the kafka-cluster that don't belong to kafkakewl, but shouldn't be reported as discrepancies between the desired and actual state.

By default it's an empty instance of [`NonKewlKafkaResources`](NonKewlKafkaResources.md).

**`requiresAuthorizationCode`**: `Boolean?` - the optional flag indicating whether changes (deployments) to this cluster requires an authorization code to avoid accidental deployments.

By default it's `false`.

**`replicaPlacementConfigs`**: [`Map[String, ReplicaPlacementConfig]?`](ReplicaPlacementConfig.md) - the optional map of replica-placement configurations to identifiers. The keys of these map can be used in the [topic](../topology/TopologyTopic.md)s' `replicaPlacement` property.

**`defaultReplicaPlacementId`**: `String?` - the optional (mandatory if there are `replicaPlacements`) replica-placement-id to be used for [topic](../topology/TopologyTopic.md)s without a `replicaPlacement` set.

**`topicConfigKeysAllowedInTopologies`**: [`TopicConfigKeyConstraintInclusive?`](TopicConfigKeyConstraints.md) - the optional topic config constraints that decide what topic configs are allowed in topologies' topics.

If empty, or if the `include` part is empty, it'll include every possible topic configs.

It's recommended to set this to something like:

```json
{
    "topicConfigKeysAllowedInTopologies": {
        "include": [
            "cleanup.policy",
            "delete.retention.ms",
            "max.compaction.lag.ms",
            "min.cleanable.dirty.ratio",
            "min.compaction.lag.ms",
            "min.insync.replicas",
            "retention.ms",
            "segment.bytes",
            "segment.ms"
        ]
  }
}
```

**`additionalManagedTopicConfigKeys`**: [`TopicConfigKeyConstraintExclusive?`](TopicConfigKeyConstraints.md) - the optional topic config constraints that in addition to the `topicConfigKeysAllowedInTopologies` decides what topic configs are managed by kafkakewl.

If a topic config is NOT managed by kafkakewl, it won't ever try to overwrite that config in a kafka topic. It's useful when kafka or confluent platform uses certain topic configs for internal tools, e.g. `leader.replication.throttled.replicas` and kafkakewl can ignore these safely.  

If empty, or if the `include` part is empty, it'll include no additional topic configs.

It's recommended to set this to something like:

```json
{
    "additionalManagedTopicConfigKeys": {
        "include": [
            "confluent.placement.constraints"
        ]
    }
}
```

so that even though `"confluent.placement.constraints"` is not allowed in topology it's still managed by kafkakewl (gets typically generated by the `replicaPlacement`). 

**`tags`**: `Array[Strings]?` - an optional array of string tags for the cluster. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the cluster. By default it's empty and it's completely ignored by kafkakewl.

# Examples

A simple test kafka-cluster without any security protocol, part of the `default` and `test` environments:
```json
{
    "kafkaCluster": "test",
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "name": "test",
    "environments": {
        "default": {},
        "test": {}
    }
}
```

Like above but using `SASL_PLAINTEXT` as security protocol:
```json
{
    "kafkaCluster": "test",
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "securityProtocol": "SASL_PLAINTEXT",
    "name": "test",
    "environments": {
        "default": {},
        "test": {}
    }
}
```

A prod kafka-cluster with security protocol = `SASL_PLAINTEXT` and requiring authorization code for deployments:
```json
{
    "kafkaCluster": "prod",
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "securityProtocol": "SASL_PLAINTEXT",
    "name": "prod",
    "environments": {
        "default": {},
        "prod": {}
    },
    "requiresAuthorizationCode": true
}
```

A kafka-cluster where kafkakewl should ignore topics starting with `"debezium."`:
```json
{
    "kafkaCluster": "test",
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "name": "test",
    "environments": {
        "default": {},
        "test": {}
    },
    "nonKewl": {
        "topicRegexes": ["debezium\\..*"],
    },
}
```
