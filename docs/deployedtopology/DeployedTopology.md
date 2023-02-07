# DeployedTopology

**`kafkaClusterId`**: `String` - the kafka-cluster id where it's deployed

**`topologyId`**: `String` - the topology id

**`deploymentVersion`**: `Int` - the version of the [deployment](../deployment/Deployment.md) entity that triggered this deployed-topology

**`topologyWithVersion`**: `Object` - the [topology](../topology/Topology.md) (**`topology`** field) that's deployed with its version (**`version`** field)

**`allActionsSuccessful`**: `Boolean` - true if all deployment actions were successful at the last deployment

**`notRemovedKeys`**: `Array[String]` - the keys of topics and ACLs which should have been removed but they weren't

This is for internal use, users can ignore it.

**`kafkaClusterActions`**: `Array[KafkaClusterAction]` - an array of actions that were performed in the kafka-cluster as the last deployment of this topology

The `KafkaClusterAction` has the following fields:
- **`action`**: `String` - a human readable description of the action that was performed
- **`safety`**: `String` - describes whether the action was `Safe`, `UnsafeNotAllowed`, `UnsafeAllowed` or `UnsafePartiallyAllowed`
- **`notAllowedUnsafeChange`**: [`NotAllowedUnsafeChange?`](NotAllowedUnsafeChange.md) - if the unsafe action was not allowed or partially allowed it describes the details of it, otherwise null

- **`execution`**: `Object?` - if any action was executed, it describes it, otherwise null

  - **`action`**: `String` - the action that was actually executed
  - **`success`**: `Boolean` - true if the action was executed successfully
  - **`result`**: `String` - the textual result of the action execution (error)

**`tags`**: `Array[Strings]?` - an optional array of string tags for the deployed-topology. By default it's empty, and it's completely ignored by kafkakewl.

**`labels`**: `Map[String, String]?` - an optional object containing string key-values for custom labels for the deployed-topology. By default it's empty and it's completely ignored by kafkakewl.

# Example

A deployed-topology whose last deployment re-created the `test-topic-1` topic:
```json
{
  "kafkaClusterId": "prod",
  "topologyId": "projectx",
  "deploymentVersion": 50,
  "topologyWithVersion": {
    "version": 80,
    "topology": {
      "namespace": "projectx",
      "topology": "",
      "developers": [
        "my-user-name"
      ],
      "developersAccess": "TopicReadOnly",
      "deployWithAuthorizationCode": true,
      "topics": {
        "test-topic-1": {
          "name": "projectx.test-topic-1",
          "partitions": 2,
          "replicationFactor": 3,
          "config": {
            "cleanup.policy": "compact"
          },
        }
      },
      "applications": {
        "my-producer": {
          "user": "my-producer-service-user",
          "type": {
            "consumerGroup": null,
            "transactionalId": null
          },
          "consumerLagWindowSeconds": null
        },
        "my-consumer": {
          "user": "my-consumer-service-user",
          "type": {
            "consumerGroup": "projectx.my-consumer",
            "transactionalId": null
          },
          "consumerLagWindowSeconds": null
        }
      },
      "aliases": {
        "topics": {},
        "applications": {}
      },
      "relationships": {
        "my-producer": {
          "produce": [
            "test-topic-1"
          ]
        },
        "my-consumer": {
          "consumer": [
            "test-topic-1"
          ]
        }
      }
    }
  },
  "allActionsSuccessful": true,
  "notRemovedKeys": [],
  "kafkaClusterActions": [
    {
      "action": "delete: [projectx.test-topic-1, partitions=2, replicationFactor=3, config=Map(cleanup.policy -> compact)]",
      "safety": {
        "UnsafeAllowed": {}
      },
      "notAllowedUnsafeChange": null,
      "execution": {
        "action": "delete: [projectx.test-topic-1, partitions=2, replicationFactor=3, config=Map(cleanup.policy -> compact)]",
        "success": true,
        "result": "deleted: projectx.test-topic-1"
      }
    },
    {
      "action": "add: [projectx.test-topic-1, partitions=2, replicationFactor=3, config=Map(cleanup.policy -> compact)]",
      "safety": {
        "Safe": {}
      },
      "notAllowedUnsafeChange": null,
      "execution": {
        "action": "add: [projectx.test-topic-1, partitions=2, replicationFactor=3, config=Map(cleanup.policy -> compact)]",
        "success": true,
        "result": ""
      }
    }
  ]
}
```