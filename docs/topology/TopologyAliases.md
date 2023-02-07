# Topology / Aliases

The aliases object can define additional topic/application identifiers for the current topology which can point to zero or more real topics/applications, either external or local or both. The purpose of these additional topic/application ids is that they can be used in relationships, for example:

- when you need an alias for a list of local or external topics, because you want to use them in relationships and it's more compact this way (see the `list-of-topics` example)
- useful when you consume or produce a dynamic set of topics (local or external), see `all-external-projecty-topics` example
- useful when all local applications wants to consume or produce a topic (e.g. error topic), see `applications-all` example

**`topics`**: [`Map[String, Array[FlexibleId]]`](../FlexibleId.md) - an optional set of aliases to kafka-topics in this topology

A topic alias can point to zero or more [FlexibleIds](../FlexibleId.md).

**`applications`**: [`Map[String, Array[FlexibleId]]`](../FlexibleId.md) - an optional set of aliases to kafka-topics in this topology

An application alias can point to zero or more [FlexibleIds](../FlexibleId.md).

# Examples

```json
{
  "aliases": {
    "topics": {
      "topics-all": [ { "namespace": "projectx" } ],
      "list-of-topics": [ "local-topic1", "local-topic2" ],
      "all-external-projecty-topics": [ { "namespace": "projecty" } ]
    },
    "applications": {
      "applications-all": [ { "namespace": "projectx" } ],
    }
  }
}
```