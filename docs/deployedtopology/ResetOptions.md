# DeployedTopology / ResetOptions

**`application`**: `String` - the local application id of the application to be reset

**`options`**: `ResetApplicationOptions` - the reset application options

For Details see below.

**`authorizationCode`**: `String?` - the optional authorization code (for some topologies / kafka-clusters it's mandatory to confirm the deployment by entering the authorization code that kafkakewl generated)

## ResetApplicationOptions

It's an object that can have one the following keys and the fields inside the object for the key:

### `"ApplicationTopics"`

**`topics`**: [`Array[FlexibleTopicId]`](../FlexibleTopicId.md) - the list of topics to be reset for this application.

If it's empty (**default**) all topics' offsets consumed by the application are reset.

**`position`**: `TopicPartitionPosition?` - specifies where it should reset the application's offsets to

For details see below.

**`deleteKafkaStreamsInternalTopics`**: [`Array[FlexibleName]?`](../FlexibleName.md) - an optional list of [flexible-names](../FlexibleName.md) to filter the kafka-streams internal topics to be deleted

By default it's `[{"any": true}]`, so all kafka-streams internal topics will be deleted.

### `"ApplicationTopicPositions"`

**`positions`**: `Map[String, TopicPartitionPosition]` - specifies by local topic id where it should reset the application's offsets to

For details see below.

**`deleteKafkaStreamsInternalTopics`**: [`Array[FlexibleName]?`](../FlexibleName.md) - an optional list of [flexible-names](../FlexibleName.md) to filter the kafka-streams internal topics to be deleted

By default it's `[{"any": true}]`, so all kafka-streams internal topics will be deleted.

### `"ApplicationTopicPartitions"`

**`positions`**: `Map[String, Array[TopicPartitionPositionOfPartition]]` - specifies by local topic id where it should reset the application's topic-partition offsets to

For details see below.

**`deleteKafkaStreamsInternalTopics`**: [`Array[FlexibleName]?`](../FlexibleName.md) - an optional list of [flexible-names](../FlexibleName.md) to filter the kafka-streams internal topics to be deleted

By default it's `[{"any": true}]`, so all kafka-streams internal topics will be deleted.

### `"Connector"`

**`keyRegex`**: `String?` - an optional regex to filter the connector keys to reset. By default it doesn't filter, but resets all keys.

### `"ConnectReplicator"`

**`topic`**: `String?` an optional topic name to filter the reset to. By default it doesn't filter, but resets all topics.

**`partition`**: `Int?` an optional partition id to filter the reset to. By default it doesn't filter, but resets all topic-partitions.

## TopicPartitionPosition

It can be one of the following:
- `{ "default": true }` - default behavior which is `Beginning`
- `{ "beginning": true }` - resets to the beginning of the topic
- `{ "end": true }` - resets to the end of the topic
- `{ "offset": [offset-value-as-long] }` - resets to the specific `offset-value-as-long`
- `{ "timeStamp": "[timestamp-in-iso-format]" }` - resets to the specific `timestamp-in-iso-format`, for example `"2020-05-01T00:00:00.000Z"`

## TopicPartitionPositionOfPartition

**`partition`**: `Int` - the partition id for the reset-position

**`position`**: `TopicPartitionPosition` - the position to reset the partition to

For details see above.

# Examples

Resetting an application's all consumer group offsets to their default reset mode specified in the consume relationship or `Beginning` if it's not specified:

```json
{
  "application": "my-consumer",
  "options": {
    "ApplicationTopics": {
    }
  }
}
```

Like above, but resetting only topic1's offsets:
```json
{
  "application": "my-consumer",
  "options": {
    "ApplicationTopics": {
      "topics": ["topic1"]
    }
  }
}
```

Resetting an application's topic1's consumer group offsets to the beginning:
```json
{
  "application": "my-consumer",
  "options": {
    "ApplicationTopics": {
      "topics": ["topic1"],
      "position": { "beginning": true }
    }
  }
}
```

Resetting an application's topic1's consumer group offsets to the beginning and topic2's to the end:
```json
{
  "application": "my-consumer",
  "options": {
    "ApplicationTopicPositions": {
      "positions": {
        "topic1": { "beginning": true },
        "topic2": { "end": true }
      }
    }
  }
}
```

Resetting an application's topic1's consumer group offsets to `"2020-05-01T00:00:00.000Z"` in partition 0 and `"2020-06-01T00:00:00.000Z"` in partition 1:
```json
{
  "application": "my-consumer",
  "options": {
    "ApplicationTopicPartitions": {
      "positions": {
        "topic1": [
          { "partition": 0, "position": { "timeStamp": "2020-05-01T00:00:00.000Z" } },
          { "partition": 1, "position": { "timeStamp": "2020-06-01T00:00:00.000Z" } }
        ]
      }
    }
  }
}
```