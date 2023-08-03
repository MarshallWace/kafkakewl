# DeployedTopologyMetrics

**`kafkaClusterId`**: `String` - the kafka-cluster id where it's deployed

**`topologyId`**: `String` - the topology id

**`aggregateConsumerGroupStatus`**: `ConsumerGroupStatus` **DEPRECATED** - the aggregated overall consumer group status for the whole topology, for all applications' consumer groups

For details see below. It's DEPRECATED, please use only the `aggregatedConsumerGroupStatus`.

**`aggregatedConsumerGroupStatus`**: `AggregatedConsumerGroupStatus?` - the optional aggregated overall consumer group status for the whole topology, for all applications' consumer groups

For details see below.

**`topics`**: [`Map[String, TopicInfo?]`](../deployedtopology/DeployedTopicInfo.md) - an object containing the kafka-cluster's topic information by the topic identifiers

**`applications`**: `Map[String, ApplicationInfo?]` - an object containing the kafka-cluster's topic information by the topic identifiers

The **`ApplicationInfo`** object has the following properties:

- `consumerGroupStatus`: `ApplicationConsumerGroupInfo?` - the consumer group info or null if it's not available

For details see below.

## ConsumerGroupStatus

It can be one of the following:
- `Ok` - there are new offsets committed for the consumer group and the lag has decreased at least once (or was zero) in the last monitoring window
- `Unknown` - kafkakewl can't tell the status of the consumer group right now (it typically happens right after start-up when it doesn't have enough data-points about the lag)
- `Warning` - there are new offsets committed for the consumer group but the lag is increasing in the last monitoring window
- `Error` - there are no new offsets committed, but there are offsets committed (the same ones) in the last monitoring window - the consumer is not stopped, but not making progress
- `MaybeStopped` - **NA** - there are no new offsets committed, maybe the consumer doesn't commit at all or commits the same offset in the last monitoring window - most likely it's stopped
- `Stopped` - the consumer hasn't committed any new offset in the last monitoring window

Note that the monitoring window is 5 minutes by default. It can be changed on the [topology's application](../topology/TopologyApplication.md) with the `consumerLagWindowSeconds` property.

Note the `MaybeStopped` is not applicable right now: they work only if kafkakewl polled the consumer groups' for the latest offsets which it doesn't do, but consumes the `__consumer_offsets` topic.

## AggregatedConsumerGroupStatus

**`best`**: `ConsumerGroupStatus` - the "best" consumer group status across all consumed topic-partitions in this topology

The order from the best to worst can be seen above, from `Ok` to `Stopped`.

**`worst`**: `ConsumerGroupStatus` - the "worst" consumer group status across all consumed topic-partitions in this topology

The order from the best to worst can be seen above, from `Ok` to `Stopped`.

## ApplicationConsumerGroupInfo

**`aggregateConsumerGroupStatus`**: `ConsumerGroupStatus` **DEPRECATED** - the aggregated overall consumer group status for the application's consumer group

For details see above. It's DEPRECATED, please use only the `aggregatedConsumerGroupStatus`.

**`aggregatedConsumerGroupStatus`**: `AggregatedConsumerGroupStatus?` - the optional aggregated overall consumer group status for application's consumer group

For details see above.

**`topics`**: `Map[String, ApplicationConsumerGroupTopicInfo]` - an object containing the topics' consumer group related info by the topic identifiers

## ApplicationConsumerGroupTopicInfo

**`aggregateConsumerGroupStatus`**: `ConsumerGroupStatus` **DEPRECATED** - the aggregated overall consumer group status for the application's consumer group for the specified topic

For details see above. It's DEPRECATED, please use only the `aggregatedConsumerGroupStatus`.

**`aggregatedConsumerGroupStatus`**: `AggregatedConsumerGroupStatus?` - the optional aggregated overall consumer group status for application's consumer group for the specified topic

**`totalLag`**: `Long?` - the total lag across all partitions of the topic or null if it's not available

**`partitions`**: `Map[String, ConsumerGroupMetrics]` - the consumer group metrics by the partition ids as strings

For details see below.

## ApplicationTopicConsumerGroupMetrics

**`status`**: `ConsumerGroupStatus` - the topic-partition's consumer group's status

**`partitionHigh`**: `PartitionHighOffset?` - the topic-partition's high offset (`offset` field) and the timestamp (`lastOffsetsTimestampUtc` field) or null if it's not available

**`committed`**: `ConsumerGroupOffset?` - the topic-partition's consumer group's offset (`offset` field), the metadata (`metadata` field) and the timestamp (`lastOffsetsTimestampUtc` field) or null if it's not available

**`lag`**: `Long?` - the lag between the high offset and the committed offset or null if it's not available

# Examples

```json
{
  "kafkaClusterId": "prod",
  "topologyId": "projectx",
  "aggregateConsumerGroupStatus": "MaybeStopped",
  "aggregatedConsumerGroupStatus": {
    "best": "Ok",
    "worst": "MaybeStopped"
  },
  "topics": {
    "projectx.test-topic-1": {
      "topic": "projectx.test-topic-1",
      "partitions": {
        "0": {
          "lowOffset": 0,
          "highOffset": 510,
          "metrics": {
            "incomingMessagesPerSecond": 10.121,
            "lastOffsetTimestampUtc": "2020-06-04T14:12:23.222Z"
          }
        },
        "1": {
          "lowOffset": 0,
          "highOffset": 89,
          "metrics": {
            "incomingMessagesPerSecond": 4.51,
            "lastOffsetTimestampUtc": "2020-06-04T14:12:11.001Z"
          }
        }
      },
      "metrics": {
        "incomingMessagesPerSecond": 13.51,
        "lastOffsetTimestampUtc": "2020-06-04T14:12:23.222Z"
      }
    }
  },
  "applications": {
    "projectx.my-producer": {
      "consumerGroupStatus": null
    },
    "projectx.my-consumer": {
      "consumerGroupStatus": {
        "aggregateConsumerGroupStatus": "MaybeStopped",
        "aggregatedConsumerGroupStatus": {
          "best": "Ok",
          "worst": "MaybeStopped"
        },
        "topics": {
          "projectx.test-topic-1": {
            "aggregateConsumerGroupStatus": "MaybeStopped",
            "aggregatedConsumerGroupStatus": {
              "best": "Ok",
              "worst": "MaybeStopped"
            },
            "totalLag": 40,
            "partitions": {
              "0": {
                "status": "Ok",
                "partitionHigh": {
                  "offset": 510,
                  "lastOffsetsTimestampUtc": "2020-06-04T14:12:23.222Z"
                },
                "committed": {
                  "offset": 510,
                  "metadata": "",
                  "lastOffsetsTimestampUtc": "2020-06-04T14:14:10.162Z"
                },
                "lag": 0
              },
              "1": {
                "status": "MaybeStopped",
                "partitionHigh": {
                  "offset": 89,
                  "lastOffsetsTimestampUtc": "2020-06-04T14:12:11.001Z"
                },
                "committed": {
                  "offset": 49,
                  "metadata": "",
                  "lastOffsetsTimestampUtc": "2020-06-04T14:01:00.011Z"
                },
                "lag": 40
              }
            }
          }
        }
      }
    }
  }
}
```