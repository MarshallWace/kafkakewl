# KafkaCluster / NonKewlKafkaResources / NonKewlKafkaAcl

This allows a more fine-grained description of a non-kewl kafka acls.

All fields are optional. If any is missing, it's considered to match any acls.

All regular expressions here are interpreted as whole-string regexes by default (thus containment should be expressed like ".*topic-name-fragment.*")

The possible values are exactly the same as the possible values of the corresponding kafka acl attributes.

**`resourceType`**: `String?` - the optional resource-type of the non-kewl acl.

Can be one of the following:
- `UNKNOWN`
- `ANY`
- `TOPIC`
- `GROUP`
- `CLUSTER`
- `TRANSACTIONAL_ID`
- `DELEGATION_TOKEN`

For details see [kafka resource types](https://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/common/resource/ResourceType.html)

**`resourcePatternType`**: `String?` - the optional resource pattern-type of the non-kewl acl.

Can be one of the following:
- `UNKNOWN`
- `ANY`
- `MATCH`
- `LITERAL`
- `PREFIXED`

For details see [kafka resource pattern types](https://kafka.apache.org/20/javadoc/org/apache/kafka/common/resource/PatternType.html)

**`resourceNameRegex`**: `String?` - the optional regular expression matching the non-kewl acl.

**`principal`**: `String?` - the optional principal of the non-kewl acl.

**`host`**: `String?` - the optional host of the non-kewl acl.

**`operation`**: `String?` - the optional operation of the non-kewl acl.

Can be one of the following:
- `UNKNOWN`
- `ANY`
- `ALL`
- `READ`
- `WRITE`
- `CREATE`
- `DELETE`
- `ALTER`
- `DESCRIBE`
- `CLUSTER_ACTION`
- `DESCRIBE_CONFIGS`
- `ALTER_CONFIGS`
- `IDEMPOTENT_WRITE`

For details see [kafka acl operations](http://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/common/acl/AclOperation.html)

**`permission`**: `String?` - the optional permission of the non-kewl acl.

Can be one of the following:
- `UNKNOWN`
- `ANY`
- `DENY`
- `ALLOW`

For details see [kafka acl permission types](http://kafka.apache.org/20/javadoc/index.html?org/apache/kafka/common/acl/AclPermissionType.html)
