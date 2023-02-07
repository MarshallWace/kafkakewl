# DeployedTopology / DeployedTopicInfo

**`topic`**: `String` - the topic name
**`partitions`**: `Map[String, TopicPartitionInfo]` - the topic partitions, keyed by the partition ids as `Strings`

For details see below.

**`metrics`**: `Metrics?` - the topic's metrics, or null if it's not available

For details see below.

## TopicPartitionInfo

**`lowOffset`**: `Long` - the low offset of the partition

**`highOffset`**: `Long` - the high offset of the partition

**`metrics`**: `Metrics?` - the partition's metrics, or null if it's not available

For details see below.

## Metrics

**`incomingMessagesPerSecond`**: `Double` - the incoming messages per second in the last 60 seconds

**`lastOffsetTimestampUtc`**: `String` - the timestamp of the last new offset/message of the topic
