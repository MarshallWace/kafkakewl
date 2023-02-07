# Topology / Application

**`user`**: `String` - the user who runs this application

This user will be used in kafka ACLs as a `User` principal. Environment variables can be used.

**`host`**: `String?` - the optional host-name where the application is going to run, by default empty.

If specified, only this host will be allowed in kafka to consume or produce the topics. Typically it remains unspecified. At the moment it cannot contain environment variable.

**`type`**: `Object?` - optional object describing the application-type specific properties.

By default it's a simple one, without a consumer group and transactional id. The possible values:

- *simple*
  - `consumerGroup`: `String?` - the optional consumer group that this application uses to consume kafka topics

    By default it's null, meaning there is no consumer group (e.g. for produce-only applications). If it's specified, it must be part of the topology `namespace`, like topic names. If this application has a consume relationship with any topic, it'll get a specific kafka ACL to be able to READ this consumer group. At the moment it cannot contain environment variables.
  - `transactionalId`: `String?`: the optional transactional id prefix that this application uses to produce kafka
    topics

    By default it's null, meaning it won't get and kafka ACL for transactional ids. Like above, if specified, it must be part of the topology `namespace`, like topic names. At the moment it cannot contain environment variables.

- *kafka-streams*
  - `kafkaStreamsAppId`: `String` - the kafka-streams application's id

   Like above, it must be part of the topology `namespace`, like topic names. It'll make the application get all the necessary kafka ACLs to work with kafka-streams. At the moment it cannot contain environment variables.

- *connector*
  - `connector`: `String` - the kafka connector id

    It will be used e.g. when you want to reset the connector. At the moment it cannot contain environment variables.

- *connect-replicator*
  - `connectReplicator`: `String` - the connect-replicator id

    It will be used e.g. when you want to reset the connect-replicator. At the moment it cannot contain environment variables.

**`description`**: `String?` - the optional description of the application

**`otherConsumableNamespaces`**: `Array[FlexibleName]` - an optional list of [`flexible names`](../FlexibleName.md) that define the other topology namespaces whose topics can be consumed by this application.

It should be a rare use-case, but this way a topology can expose a consuming application, and other topologies can add consume relationship between this application and their topics. By default it's empty, which means it's not exposed at all.

**`otherProducableNamespaces`**: `Array[FlexibleName]` - an optional list of [`flexible names`](../FlexibleName.md) that define the other topology namespaces whose topics can be produced by this application.

It's even less typical, mostly there for consistency.

**`consumerLagWindowSeconds`**: `Int?` - an optional number of seconds, which indicates the consumer lag window size for the metrics monitoring.

Environment variables can be used. If not specified, it's 5 minutes (300 seconds). It means that the lag is recorded for the last 5 minutes and the consumer group status is calculated based on that.

**`tags`**: `Array[Strings]?` - an optional array of string tags for the application. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the application. By default it's empty and it's completely ignored by kafkakewl.

# Examples

An application that can only be a non-transactional producer (there is no `type` specified, so it defaults to an empty `simple` application with no consumer group and transactional id):
```json
{
  "applications": {
    "my-producer": {
      "user": "service-user-name"
    }
  }
}
```

An application that can be a consumer of topics with the `projectx.my-consumer` group, but can be only a non-transactional producer (the transactional id is not specified):
```json
{
  "applications": {
    "my-consumer": {
      "user": "service-user-name",
      "type": {
        "consumerGroup": "projectx.my-consumer"
      }
    }
  }
}
```

An application that can be a consumer of topics with the `projectx.my-consumer` group, and can be a transactional producer with the `projectx.my-producer` transactional id or a non-transactional producer:
```json
{
  "applications": {
    "my-consumer": {
      "user": "service-user-name",
      "type": {
        "consumerGroup": "projectx.my-consumer",
        "transactionalId": "projectx.my-producer"
      }
    }
  }
}
```

A kafka-streams application with the `projectx.my-kafka-streams-app` application id:
```json
{
  "applications": {
    "my-consumer": {
      "user": "service-user-name",
      "type": {
        "kafkaStreamsAppId": "projectx.my-kafka-streams-app"
      }
    }
  }
}
```
