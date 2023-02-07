# Topology / Relationship

**`consume`**: `Array[Participant]?` - an array of other participants that the main participant consumes (the main participants is the key of this object)

**`produce`**: `Array[Participant]?` - an array of other participants that the main participant produces (the main participants is the key of this object)

**`{any-other-custom-key}`**: `Array[Participant]?` - an array of other participants that the main participant have a custom relationship with (the main participants is the key of this object)

kafkakewl ignores custom relationships when manipulating topics/ACLs in the kafka cluster

## Participant

### Simplified syntax without custom properties for the participant:

Here the other participant is simply a `String` indicating the identifier of the other participant of the relationship. It can be an application or topic (local or external) or even an alias:

```json
{
  "relationships": {
    "my-consumer": {
      "consume": [
        "participant1-id",
        "participant2-id"
      ]
    }
  }
}

```

### Full syntax supporting custom properties for the participant:

Here the other participant is an object with a single key: the identifier of the other participant of the relationship. Like above it can be an application or topic (local or external) or even an alias. The value is an object with the following optional properties:

**`reset`**: `String?` - specifies how this should be reset, when the main application participant is reset. It's valid only for application-consuming-topic relationships.

It can be the following:
- `ignore`: do not do anything when the application is reset
- `beginning` (**default**): reset the consumer group offset to the beginning of the topic
- `end`: reset the consumer group offset to the current end of the topic

**`monitorConsumerLag`**: `Boolean?` - an optional boolean flag that can be set only for consume relationships. If set to `true` (**default**), then this consumption is expected to keep up with the topic and if it doesn't, that's an error (lagging behind) in the consumer group status.

Sometimes it makes sense to set it to `false`, when the application consumes the topic, but not constantly and isn't supposed to be up-to-date (e.g. consumes it at start-up only, etc...). These applications lagging behind is not a problem, so it won't be reported as an error in the UI.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the relationship. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the relationship. By default it's empty and it's completely ignored by kafkakewl.

```json
{
  "relationships": {
    "my-consumer": {
      "consume": [
        {
          "participant1-id": {
            "reset": "end",
            "tags": [ "tag1" ]
          }
        }
      ]
    }
  }
}

```

# Examples

Consume and produce relationship for the same application:

```json
{
  "relationships": {
    "my-app": {
      "consume": [ "data-source-topic" ],
      "produce": [ "data-sink-topic" ]
    }
  }
}
```

Relationship between two applications. Like above but `my-app` "consumes" `my-app-other` - it means nothing for kafkakewl, but shows up in the UI. Can be useful for kafka-streams topologies:

```json
{
  "relationships": {
    "my-app": {
      "consume": [ "data-source-topic", "my-app-other" ],
      "produce": [ "data-sink-topic" ]
    }
  }
}
```


Combining the two syntaxes in the same relationship, if you want to set custom relationship properties:

```json
{
  "relationships": {
    "my-app": {
      "consume": [
        "data-source-topic-1",
        {
          "data-source-topic-2": {
            "monitorConsumerLag": false
          }
        }
      ],
      "produce": [ "data-sink-topic" ]
    }
  }
}
```
