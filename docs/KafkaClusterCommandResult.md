# KafkaClusterCommandResult

**`metadata`**: [`CommandMetadata`](CommandMetadata.md) - the command metadata.

**`kafkaClusterId`**: `String` - the kafka-cluster id where the command was executed

in addition to these:

## If it has failed

**`reasons`**: [`Array[CommandError]`](CommandError.md) - the errors that occurred

**`response`**: [`KafkaClusterCommandResponse`](KafkaClusterCommandResponse.md) - the command response (even if it failed it can have some partial results)

## If it has succeeded

**`response`**: [`KafkaClusterCommandResponse`](KafkaClusterCommandResponse.md) - the command response
