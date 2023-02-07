# KafkaCluster / NonKewlKafkaResources

It defines all the kafka topics and acls that are NOT part of kafkakewl and should NOT be reported as discrepancies between the desired and actual state of the kafka-cluster.

All regular expressions here are interpreted as whole-string regexes by default (thus containment should be expressed like ".*topic-name-fragment.*")

**`topicRegexes`**: `Array[String]` - regular expressions defining the non-kewl topic names.

**`topicForAclsRegexes`**: `Array[String]` - regular expressions defining the non-kewl topic acls by their associated topic-name.

**`groupForAclsRegexes`**: `Array[String]` - regular expressions defining the non-kewl topic acls by their associated topic-name.

**`transactionalIdForAclsRegexes`**: `Array[String]` - regular expressions defining the non-kewl transactional ids.

**`kafkaAcls`**: [`Array[NonKewlKafkaAcl]`](NonKewlKafkaAcl.md) - non-kewl kafka ACLs

# Example:

Ignoring topics starting with `"kewl."`
```json
{
  "nonKewl": {
    "topicRegexes": ["kewl\\..*"]
  }
}
```

Ignoring topics starting with `"kewl."`, and their associated ACLs too:
```json
{
  "nonKewl": {
    "topicRegexes": ["kewl\\..*"],
    "topicForAclsRegexes": ["kewl\\..*"]
  }
}
```

Ignoring a specific ACL in the kafka-cluster:
```json
{
  "nonKewl": {
    "kafkaAcls": [
      {
        "resourceType": "TOPIC",
        "resourcePatternType": "PREFIXED",
        "resourceNameRegex": "some-topics-.*",
        "principal": "joe",
        "host": "*",
        "operation": "WRITE",
        "permission": "ALLOW"
      }
    ]
  }
}
```