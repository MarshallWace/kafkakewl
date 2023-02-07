# DeployedTopology / DeployedApplication

**`applicationId`**: `String` - the local application id

**`application`**: [`TopologyApplication`](../topology/TopologyApplication.md) - the application itself without any variables in any properties

This is the "resolved" version of the original application in the topology.

**`consumerGroupInfo`**: `ConsumerGroupInfo?` - the consumer group related properties from the kafka-cluster or null if it's not available

For details see below.

## ConsumerGroupInfo

**`consumerGroup`**: `String` - the consumer group name

**`isSimple`**: `Boolean` - true if it's a non-rebalancing, simple consumer group

**`members`**: `Array[ConsumerGroupMember]` - the consumer group members

For details see below.

**`partitionAssignor`**: `String` - the kafka partition assignor

**`state`**: `String` - the kafka consumer group state

Can be one of the following:
- `Unknown`
- `PreparingRebalance`
- `CompletingRebalance`
- `Stable`
- `Dead`
- `Empty`

**`coordinator`**: `KafkaNode` - the kafka consumer group coordinator node

- `id`: `Int`
- `host`: `String`
- `port`: `Int`
- `rack`: `String?`

**`topics`**: `Map[String, ConsumerGroupTopicInfo]` - the consumer group's topics by topic-name

For details see below.

## ConsumerGroupMember

**`consumerId`**: `String` - the consumer id

**`clientId`**: `String` - the client id

**`host`**: `String` - the host

**`assignment`**: `Array[TopicPartition]` - the array of topic-partitions (**`topic`**: `String` and **`partition`**: `Int` fields in each object)

## ConsumerGroupTopicInfo

**`topic`**: `String` - the topic name

**`partitions`**: `Map[String, ConsumerGroupTopicPartitionInfo]` - the topic partitions, keyed by the partition ids as `Strings`

For details see below.

## ConsumerGroupTopicPartitionInfo

**`partition`**: [`TopicPartitionInfo`](DeployedTopicInfo.md) - the topic partition with the `metrics` field being `null` (metrics are available only via the metrics end-points)

See the `DeployedTopology / DeployedTopicInfo / TopicPartitionInfo` section in [`DeployedTopicInfo`](DeployedTopicInfo.md)

**`committedOffset`**: `Long?` - the committed offset for this partition or null if there isn't any

# Examples

A simple consumer application:
```json
{
  "applicationId": "my-consumer",
  "application": {
    "user": "my-consumer-service-user",
    "type": {
      "consumerGroup": "projectx.my-consumer",
      "transactionalId": null
    },
    "consumerLagWindowSeconds": null
  },
  "consumerGroupInfo": {
    "consumerGroup": "projectx.my-consumer",
    "isSimple": true,
    "members": [],
    "partitionAssignor": "",
    "state": "Empty",
    "coordinator": {
      "id": 1,
      "host": "broker1",
      "port": 9092,
      "rack": null
    },
    "topics": {
      "test-topic-1": {
        "topic": "projectx.test-topic-1",
        "partitions": {
          "0": {
            "partition": {
              "lowOffset": 0,
              "highOffset": 403,
              "metrics": null
            },
            "committedOffset": 335
          },
          "1": {
            "partition": {
              "lowOffset": 0,
              "highOffset": 80,
              "metrics": null
            },
            "committedOffset": 49
          }
        }
      }
    }
  }
}
```