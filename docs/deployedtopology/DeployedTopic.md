# DeployedTopology / DeployedTopic

**`topicId`**: `String` - the local topic id

**`topic`**: [`TopologyTopic`](../topology/TopologyTopic.md) - the topic itself without any variables in any properties

This is the "resolved" version of the original topic in the topology.

**`kafkaTopicInfo`**: [`DeployedTopicInfo`](DeployedTopicInfo.md) - the topic's other properties coming from the kafka-cluster.

A simple consumer topic:
```json
{
  "topicId": "test-topic-1",
  "topic": {
    "name": "projectx.test-topic-1",
    "partitions": 2,
    "config": {
      "cleanup.policy": "compact"
    }
  },
  "kafkaTopicInfo": {
    "topic": "projectx.test-topic-1",
    "partitions": {
      "0": {
        "lowOffset": 0,
        "highOffset": 510,
        "metrics": null
      },
      "1": {
        "lowOffset": 0,
        "highOffset": 78,
        "metrics": null
      }
    },
    "metrics": null
  }
}
```