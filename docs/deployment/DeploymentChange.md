# DeploymentChange

This entity is sent from the client to the server, never the other way around. The server sends the [Deployment](Deployment.md) entity, when the client requests the actual deployment.

**`kafkaClusterId`**: `String?` - the optional kafka-cluster id

It's optional because for `POST` and `PUT` requests, the url path already contains the kafka-cluster id. If it's specified here, it must be the same as what's in the url path.

**`topologyId`**: `String?` - the optional topology id

It's optional because for `POST` and `PUT` requests, the url path already contains the topology id. If it's specified here, it must be the same as what's in the url path.

**`topologyVersion`**: `Object?` - specifies the desired version of the topology that is expected to be deployed to the kafka-cluster.

It can have the following values:
- `{"remove": true}`: the topology should be removed from (shouldn't be deployed to) the kafka-cluster
- `{"latestOnce": true}` (**default**): the topology's current latest version must be deployed to the kafka-cluster
- `{"latestTracking": true}`: the topology's latest version must be deployed to the kafka-cluster, automatically, whenever there is a new version
- `{"exact": [exact-topology-version-number]}`: the topology's specified version must be deployed to the kafka-cluster

**`options`**: [`DeploymentOptions?`](DeploymentOptions.md) - the optional options for the deployment

**`tags`**: `Array[Strings]?` - an optional array of string tags for the deployment. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the deployment. By default it's empty and it's completely ignored by kafkakewl.

# Examples

Deploying the current latest version of a topology:
```json
{
  "topologyVersion": { "latestOnce": true }
}
```

Deploying the current latest version of a topology with deleting and re-creating `topic1` (local identifier):
```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "topicsToRecreate": [ "topic1" ]
  }
}
```

Deploying the current latest version of a topology with specifying an authorization code:
```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "authorizationCode": "{auth-code}"
  }
}
```

Deploying the current latest version of a topology with specifying that any unsafe change is allowed:
```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "allowedUnsafeChanges": [ {} ]
  }
}
```

Deploying the current latest version of a topology with specifying that `Remove` unsafe changes are allowed:
```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "allowedUnsafeChanges": [ { "operation": "Remove" } ]
  }
}
```

Removing the topics and ACLs of this topology from the kafka-cluster:
```json
{
  "topologyVersion": "remove"
}
```
