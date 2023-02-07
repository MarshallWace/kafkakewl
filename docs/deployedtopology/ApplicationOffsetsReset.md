# DeployedTopology / ApplicationOffsetsReset

This object is returned after an application's consumer offsets were reset.

**`consumerGroup`**: `String` - the consumer group whose offsets were reset

**`topicPartitionsResetToBeginning`**: `Array[OffsetOfTopicPartition]` - the offsets of the topic-partitions which were reset to the beginning

**`topicPartitionsResetToEnd`**: `Array[OffsetOfTopicPartition]` - the offsets of the topic-partitions which were reset to the end

**`topicPartitionsResetToOther`**: `Array[OffsetOfTopicPartition]` - the offsets of the topic-partitions which were reset to anywhere else (offset or timestamp)

**`deletedTopics`**: `Array[String]` - the list of deleted topics (kafka-streams application's reset involves deleting temporary topics)

# DeployedTopology / ApplicationOffsetsReset / OffsetOfTopicPartition

**`topic`**: `String` - the topic name

**`partition`**: `Int` - the partition id

**`offset`**: `Long` - the offset
