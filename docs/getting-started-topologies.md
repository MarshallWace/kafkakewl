# Getting Started - example topologies

## A simple producer-consumer topology with a single topic

```json
{
  "namespace": "projectx",
  "topics": {
    "topic1": {"name": "projectx.topic1"}
  },
  "applications": {
    "source-producer": { "user": "projectx-service-user" },
    "sink": { "user": "projectx-service-user", "type": { "consumerGroup": "projectx.sink" } }
  },
  "relationships": {
    "source-producer": { "produce": ["topic1"] },
    "sink": { "consume": ["topic1"] }
  }
}
```

Every topology is in a namespace. The namespace can be empty too, but typically it's some kind of dot-separated sequence of strings. It's important because:
- topologies can expose their topics or applications to certain other namespaces and their topologies
- kafkakewl users' permissions are based on namespaces (who can do what with topologies)
- the topic names, consumer group ids, and other kafka resources must have the namespace as a prefix

For more details, see [topologies](./topology/Topology.md).

This topology contains a single topic named **projectx.sub-project.topic1**, with the default configuration which is partitions = 1 and replication-factor = 3. Its kafka configuration is empty, so it uses your cluster's defaults.

The topic also has an id, which is **topic1**. Note that this is a local id within this topology. The fully qualified id of this topic is **projectx.topic1** which happens to be the same as the topic name, but it doesn't have to be the case. Within this topology you can refer to this topic only with the local id, but from other topologies you need to use the fully qualified topic id.

For more details see [topics in topologies](./topology/TopologyTopic.md).

This topology also defines a producer and consumer application. The consumer has a consumer-group too (**projectx.sink**). Both applications set their user so that the user will get the right ACLs to produce/consume the topics.

For more details see [applications in topologies](./topology/TopologyApplication.md).

Like topics, applications also have ids (**source-producer**, **sink**). They work the same way as topic ids: the fully qualified id contains the namespace as the prefix.

The relationships object contains the applications' local ids as keys, and relationship objects for those. For more details, see [relationships in topologies](./topology/TopologyRelationship.md).

## Developers in topologies

```json
{
  "namespace": "projectx",
  "developers": ["joe", "sue"],
  ...
}
```

The developers field can contain an optional list of users who should be able to run all applications in this topology. These users will have all the necessary ACLs to be able to run the applications.

## Using variables

```json
{
  "namespace": "projectx",
  "environments": {
    "default": {
      "partitions": "1"
    },
    "test": {
      "partitions": "4"
    },
    "prod": {
      "partitions": "50"
    }
  },
  "topics": {
    "topic1": {"name": "projectx.topic1", "partitions": "${partitions}"}
  },
  ...
}
```

It's possible to define variables whose values depend on the environment. Then these variables can be used in certain fields. In the example above the partitions can be 1, 4 or 50, depending on the environment.

For more details, see [topologies](./topology/Topology.md).

## Exposing topics for other topologies

```json
{
  "namespace": "projectx",
  "topics": {
    "topic1": {"name": "projectx.topic1", "otherConsumerNamespaces": [{ "namespace": "projecty" }] },
    "topic1": {"name": "projectx.topic2", "otherConsumerNamespaces": [{ "any": true }] },
  },
  ...
}
```
The **projectx.topic1** topic can have consume relationship in all topologies within the **projecty** namespace, the **projectx.topic2** topic is completely public: any other topology can consume it.

Note that these don't directly result in kafka ACLs, only when another topology has a consuming application for these topics.

# Topology identifiers

So far every example topology had only a **namespace** and that acted as the unique identifier the topology. The namespace also play an important role in permissioning users for operations on topologies and expose some naming constraints: every kafka-cluster resource (topics, consumer-group ids, transactional-ids) must have the topology's namespace as a prefix.

What if we want to organize our topics an applications in **projectx** in 2 topologies? We have two options:
- create 2 topologies, one with `{ "namespace": "projectx.something", ... }`, another one with `{ "namespace": "projectx.other", ... }`

  The downside of this approach is that all topics (and consumer-group ids, transactional-ids) must have the namespace as a prefix, but maybe you want only the **projectx** prefix for some reason.

- create 2 topologies, one with `{ "namespace": "projectx", "topology": "something", ... }`, another one with `{ "namespace": "projectx", "topology": "other", ... }`

  Using the optional **topology** field we can identify more than one topologies in the same namespace. The unique ids of these topologies will be `projectx.something` and `projectx.other` (the same way as in the first approach), but the topics (and consumer-group ids, transactional-ids) are constrained to start with only **projectx**.

Note that with both approaches, the fully qualified ids of topics and applications will start with `projectx.something` and `projectx.other`, as expected.

# Submitting, deploying topologies

Once you're done with the topology json, you can submit it to **kafkakewl-api** using the REST API:

`curl -XPOST localhost:8080/topology -H "Content-Type: application/json" --compressed --data @projectx.json`

This doesn't do anything with any kafka-cluster yet, it really just uploaded the topology into kafkakewl, validated it, assigned a version number.

After this, you need to deploy it to a kafka-cluster, by creating (or updating) a [deployment](deployment/DeploymentChange.md) entity:

```json
{
  "topologyVersion": { "latestOnce": true }
}
```

Save it as deployment.json, and submit with:

`curl -XPOST localhost:8080/deployment/test/projectx -H "Content-Type: application/json" --compressed --data @deployment.json`

This should create the topics in the give topology along with the ACLs needed for the applications' and developers' users. The output describes what exactly happened.

# Making a change in the topology

If you want to make a change in your submitted and deployed topology, it's easy:
1. make the change in your json file, submit the file again the same way as before
2. re-deploy your topology with the exact same steps as before

Note that re-deploying a topology deploys only the changes, nothing more. It finds out the difference between the state of the kafka-cluster and the topology to-be-deployed and makes the necessary changes.

Some changes are considered unsafe and require confirmation from the user. These changes are:
- deleting topics or ACLs
- topic config change that can result in data-loss (e.g. reducing `retention.ms`)

To allow unsafe removals, submit this deployment:

```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "allowedUnsafeChanges": [ { "operation": "Remove" } ]
  }
}
```

For more details, see [deployment options](deployment/DeploymentOptions.md).

# Re-creating topics

It's quite common, especially during development that an existing topic needs to be deleted and re-created. It's easy to do with a deployment option:

```json
{
  "topologyVersion": { "latestOnce": true },
  "options": {
    "topicsToRecreate": [ "topic1" ]
  }
}
```

For more details, see [deployment options](deployment/DeploymentOptions.md).

# Removing a topology's deployment

First you need to submit the following deployment to instruct kafkakewl that the deployment for this kafka-cluster and topology should be removed (instead of the latest version being there):

```json
{
  "topologyVersion": "remove",
  "options": {
    "allowedUnsafeChanges": [ { "operation": "Remove" } ]
  }
}
```

Note that removals need to be explicitly allowed, otherwise they are not performed because they are considered unsafe operations.

After this the kafka-cluster should no longer have the topics and ACLs belonging to this topology.

If you need to deploy the topology again, just re-deploy with `"topologyVersion": { "latestOnce": true },` and it'll be there again.

# Removing a topology completely

If you want to completely remove the topology itself, you need two more steps after the previous one:

## Removing the deployed-topology

The previous step has removed the topology's content from the kafka-cluster, but it still keeps some state for this topology and kafka-cluster around in case you want to deploy it again. If you want to completely remove the topology, you need to remove this state first:

`curl -XDELETE localhost:8080/deployment/test/projectx --compressed`

## Removing the topology itself

Finally the topology itself can be removed

`curl -XDELETE localhost:8080/topology/projectx --compressed`
