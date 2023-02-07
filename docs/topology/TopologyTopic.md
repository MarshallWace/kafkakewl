# Topology / Topic

**`name`**: `String` - the name of the kafka topic

It must be a string literal (cannot use variables). Also, must have the topology's `namespace` as the prefix (NOT `namespace` + ["." + `topology`], only `namespace`!). Every kafka topic naming limitation applies here: maximum 248 characters, only alpha-numeric or '.', '-', '_'.

**`unManaged`**: `Boolean?` - indicates whether the topic is not created/managed by kafkakewl but it should exist anyway.

UnManaged topics make it possible to have relationships between topics outside of kafkakewl. kafkakewl never creates/updates/removes unManaged topics. It's optional, by default it's false.

**`partitions`**: `Int?` - the number of partitions, by default it's 1

Environment variables can be used.

**`replicationFactor`**: `Int?` - the optional kafka replication factor, by default it's not set. It can only be set if the topic doesn't end up having a `replicaPlacement` (replica-placements and replication-factor cannot be set together for topics). If defaults to 3 if not set and there is no `replicaPlacement`.

Environment variables can be used.

**`replicaPlacement`**: `String?` - the optional replica placement id, by default it's not set (will use the kafka-cluster's `defaultReplicaPlacementId` if there is any). If it's set, it must exist in the [kafka-cluster](../kafkacluster/KafkaCluster.md)'s `replicaPlacements` map as a key.

Environment variables can be used.

**`config`**: `Map[String, String]?` - the optional kafka topic config

The usual kafka config keys and values can be used here. Note that both the keys and values must be strings. Environment variables can be used for the config values only (the config keys must be string literals).

**`description`**: `String?` - the optional description of the topic

**`otherConsumerNamespaces`**: `Array[FlexibleName]` - an optional list of [`flexible names`](../FlexibleName.md) that define the other topology namespaces whose applications can consume this topic.

By default it's empty, which means that no external application can consume this topic. It's good to make it as narrow as possible, e.g. be careful with {"any": true}.

**`otherProducerNamespaces`**: `Array[FlexibleName]` - n optional list of [`flexible names`](../FlexibleName.md) that define the other topology namespaces whose applications can produce this topic.

It's less typical, but still a valid use-case to share a topic for other external applications for writing.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the topic. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the topic. By default it's empty and it's completely ignored by kafkakewl.

# Examples

A private (can be used only in the current topology) topic with 4 partitions and one day retention:
```json
{
  "topics": {
    "test-topic": {
      "name": "projectx.test-topic",
      "partitions": 4,
      "config": {
        "retention.ms": "86400000"
      }
    }
  }
}
```

A topic that's exposed to the every topology in the `projecty` namespace for consumption:
```json
{
  "topics": {
    "test-topic": {
      "name": "projectx.test-topic",
      "otherConsumerNamespaces": [{ "namespace": "projecty" }]
    }
  }
}
```

A topic that's exposed to any topologies:
```json
{
  "topics": {
    "test-topic": {
      "name": "projectx.test-topic",
      "otherConsumerNamespaces": [{ "any": true }]
    }
  }
}

```
