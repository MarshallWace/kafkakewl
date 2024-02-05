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

Every topic can choose a single replica-placement config to use. A replica-placement config can specify defaults for topic configs and enable/disable overriding that default.

For example this can be used to fix the `confluent.placement.constraints` topic config (if the confluent platform is being used)
to a given value (e.g. 3 replicas and 3 observers for each topic with certain rack constraints) and disable overriding it in topics.
In addition to this, maybe the `min.insync.replicas` topic config can default to 2 but remain overridable in topic configs.

This way admins can give users one or more of these keyed topic config defaults and also can specify the default that topics will
use if they don't specify their `replicaPlacement`.

**`replicaPlacementConfigEquivalents`**: `Map[String, Array[String]]?` - An optional map that can specify one ore more equivalent values for a given `confluent.placement.constraints` value.

The key is a `confluent.placement.constraints` value, the value is an array of strings where each string is an equivalent `confluent.placement.constraints` value.

This is a workaround for a known confluent bug/missing feature: certain `confluent.placement.constraints` values aren't supported for new topics, e.g. in-sync replicas and observers in the same rack.
To work around it, kafkakewl can just use a `confluent.placement.constraints` that makes every replica to be an in-sync replica, and some external tool
can downgrade some of those in-sync replicas to be observers, update the topic config and do the necessary replica manipulation.

However, it's also necessary for kafkakewl not to see the updated `confluent.placement.constraints` as a difference in topic configs, and pretend that it's the same as the desired `confluent.placement.constraints`.

To achieve this, there could be a list of equivalent `confluent.placement.constraints` specified for the one that gets into the new topics so that kafkakewl will ignore any difference that an external tool makes.

An example where new topics may be created with 3+3 in-sync replicas in two racks, but a tool downgrades 1+1 in-sync replicas to be only observers:

```json
{
    "replicaPlacementConfigEquivalents": {
        "{\"observerPromotionPolicy\":\"under-min-isr\",\"version\":2,\"replicas\":[{\"count\":3,\"constraints\":{\"rack\":\"rack1\"}},{\"count\":3,\"constraints\":{\"rack\":\"rack2\"}}],\"observers\":[]}": [
          "{\"observerPromotionPolicy\":\"under-min-isr\",\"version\":2,\"replicas\":[{\"count\":2,\"constraints\":{\"rack\":\"rack1\"}},{\"count\":2,\"constraints\":{\"rack\":\"rack2\"}}],\"observers\":[{\"count\":1,\"constraints\":{\"rack\":\"rack1\"}},{\"count\":1,\"constraints\":{\"rack\":\"rack2\"}}]}"
        ]
    }
}
```

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

In addition to these, all topic configs with a default value in `replicaPlacementConfigs` are considered managed.

If a topic config is NOT managed by kafkakewl, it won't ever try to overwrite that config in a kafka topic. It's useful when kafka or confluent platform uses certain topic configs for internal tools, e.g. `leader.replication.throttled.replicas` and kafkakewl can ignore these safely.  

If empty, or if the `include` part is empty, it'll include no additional topic configs.

Normally it's OK to leave this completely empty or even not specified so that kafkakewl won't overwrite any other topic config but the ones that somehow are part of the topic's desired state:
- either specified explicitly in the topic config (and allowed in `topicConfigKeysAllowedInTopologies`)
- or have a default value in `replicaPlacementConfigs` for the given topic.

**`systemTopicsReplicaPlacementId`**: `String?` - the optional replica-placement-id to be used for the internal, system topics (e.g. `kewl.changelog`). If empty the `defaultReplicaPlacementId` is used.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the cluster. By default, it's empty, and it's completely ignored by kafkakewl.

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
