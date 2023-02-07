# DiffResult

**`topics`**: `DiffResultTopics` - the topic differences

It has the following fields:
- `missingFromKafka`: `Array[String]` - the topics missing from kafka
- `notNeededInKafka`: `Array[String]` - the additional topics in kafka that are not needed by kafkakewl
- `differences`: Array[DiffResultTopicsDiff] - the array of differences in topics (with `kafka`: `String` and `topology`: `String` fields)

**`acls`**: `DiffResultAcls` - the ACL differences

It has the following fields:
- `missingFromKafka`: `Array[String]` - the ACLs missing from kafka
- `notNeededInKafka`: `Array[String]` - the additional ACLs in kafka that are not needed by kafkakewl
