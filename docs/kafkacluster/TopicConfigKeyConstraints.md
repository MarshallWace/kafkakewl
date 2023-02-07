# KafkaCluster / TopicConfigKeyConstraints

**`include`**: `Array[TopicConfigKeyConstraint]?` - an optional array of `TopicConfigKeyConstraint`s that defines the ones to be included in this set.

If it's empty it can either include everything or nothing, it depends on where it's used.

**`exclude`**: `Array[TopicConfigKeyConstraint]?` - an optional array of `TopicConfigKeyConstraint`s that defines the ones to be excluded from this set.

# KafkaCluster / TopicConfigKeyConstraints / TopicConfigKeyConstraint

It's very similar to [FlexibleId](../FlexibleId.md)s, but only **String literals**, **Prefix** and **Regex** is allowed.

For example:

```json
{
  "include": [
    "retention.ms",
    { "prefix":  "min." },
    { "regex":  "max.*" }
  ],
  "exclude": [
    "max.compaction.lag.ms"
  ]
}
```
