# KafkaCluster / ReplicaPlacementConfig

It defines the kafka topic config to be used for the give replica-placement. It's an object where keys are kafka topic config keys and each value is a `TopicConfigDefault`.

An example where 
- the `confluent.placement.constraints` topic config will be set to that json below, and it cannot be set in the topology's topic
- the `min.insync.replicas` topic config won't be set, and it cannot be set in the topology's topic

```json
{
  "confluent.placement.constraints": {
    "overridable": false,
    "default": "{\"observerPromotionPolicy\":\"under-min-isr\",\"version\":2,\"replicas\":[{\"count\":1,\"constraints\":{\"rack\":\"01\"}},{\"count\":1,\"constraints\":{\"rack\":\"02\"}}],\"observers\":[{\"count\":1,\"constraints\":{\"rack\":\"03\"}}]}"
  },
  "min.insync.replicas": {
    "overridable": false,
    "default": null
  }
}
```

An example where
- the `confluent.placement.constraints` topic config will be set to that json below, and it cannot be set in the topology's topic
- the `min.insync.replicas` topic config also will be set to a particular value, but it can be set in the topology's topic

```json
{
  "confluent.placement.constraints": {
    "overridable": false,
    "default": "{\"observerPromotionPolicy\":\"under-min-isr\",\"version\":2,\"replicas\":[{\"count\":1,\"constraints\":{\"rack\":\"01\"}},{\"count\":1,\"constraints\":{\"rack\":\"02\"}}],\"observers\":[{\"count\":1,\"constraints\":{\"rack\":\"03\"}}]}"
  },
  "min.insync.replicas": {
    "overridable": true,
    "default": "2"
  }
}
```
# KafkaCluster / ReplicaPlacementConfig / TopicConfigDefault

**`overridable`**: `Boolean` - true means the topology's topic can also set this config which overrides the default here. If it's false, the topology's topic can't set it and always the default here will be used.

**`default`**: `String?` - if set, this is the default value for the topic config, otherwise the topic config won't be set.


