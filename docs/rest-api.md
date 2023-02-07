## Permissions

**[`GET /permission`](permission/get.md)**: returns all permissions

**[`GET /permission/{permission-id}`](permission/get-by-id.md)**: returns the permission with the specified id

**[`POST /permission`](permission/post.md)**: creates a new permission with auto-generated id

**[`DELETE /permission`](permission/delete.md)**: deletes the permission that matches the request body

**[`POST /permission/{permission-id}`](permission/post-with-id.md)**: creates a new permission with the specified id

**[`PUT /permission/{permission-id}`](permission/put-with-id.md)**: updates the permission with the specified id

**[`DELETE /permission/{permission-id}`](permission/delete-by-id.md)**: deletes the permission with the specified id


## Kafka-clusters

**[`GET /kafkacluster`](kafkacluster/get.md)**: returns all kafka-clusters

**[`GET /kafkacluster/{kafka-cluster-id}`](kafkacluster/get-by-id.md)**: returns the specified kafka-cluster

**[`POST /kafkacluster`](kafkacluster/post.md)**: creates a new kafka-cluster

**[`POST /kafkacluster/{kafka-cluster-id}`](kafkacluster/post-put-with-id.md)**: creates or updates a kafka-cluster with the specified id

**[`PUT /kafkacluster/{kafka-cluster-id}`](kafkacluster/post-put-with-id.md)**: creates or updates a kafka-cluster with the specified id

**[`DELETE /kafkacluster/{kafka-cluster-id}`](kafkacluster/delete-by-id.md)**: deletes the kafka-cluster with the specified id

## Topologies

**[`GET /topology`](topology/get.md)**: returns all topologies

**[`GET /topology/{topology-id}`](topology/get-by-id.md)**: returns the specified topology

**[`GET /topology/{topology-id}/{topology-version}`](topology/get-by-id-version.md)**: returns the specified topology version

**[`POST /topology`](topology/post.md)**: creates a new topology

**[`POST /topology/{topology-id}`](topology/post-put-with-id.md)**: creates or updates a topology with the specified id

**[`PUT /topology/{topology-id}`](topology/post-put-with-id.md)**: creates or updates a topology with the specified id

**[`DELETE /topology/{topology-id}`](topology/delete-by-id.md)**: deletes the topology with the specified id

## Deployment

**[`GET /deployment`](deployment/get.md)**: returns all topology deployments

**[`GET /deployment/{kafka-cluster-id}/{topology-id}`](deployment/get-by-id.md)**: returns the specified topology deployment

**[`POST /deployment/{kafka-cluster-id}/{topology-id}`](deployment/post-put-with-id.md)**: creates or updates the specified topology deployment

**[`PUT /deployment/{kafka-cluster-id}/{topology-id}`](deployment/post-put-with-id.md)**: creates or updates the specified topology deployment

**[`DELETE /deployment/{kafka-cluster-id}/{topology-id}`](deployment/delete-by-id.md)**: deletes the specified topology deployment

**[`POST /deployment/reapply/{kafka-cluster-id}/{topology-id}`](deployment/reapply-post-with-id.md)**: re-applies the specified topology deployment (re-deploys if needed)

## DeployedTopology

**[`GET /deployedtopology`](deployedtopology/get.md)**: returns all deployed topologies in all kafka-clusters

**[`GET /deployedtopology/{kafka-cluster-id}`](deployedtopology/get-by-kafka-cluster.md)**: returns all deployed topologies in the specified kafka-cluster

**[`GET /deployedtopology/{kafka-cluster-id}/{topology-id}`](deployedtopology/get-by-id.md)**: returns the specified deployed topology

**[`POST /deployedtopology/{kafka-cluster-id}/{topology-id}/reset`](deployedtopology/reset-post-with-id.md)**: resets an application in the specified deployed topology

**[`GET /deployedtopology/{kafka-cluster-id}/{topology-id}/topic`](deployedtopology/topics-get-by-id.md)**: returns the specified deployed topology's topic identifiers

**[`GET /deployedtopology/{kafka-cluster-id}/{topology-id}/topic/{local-topic-id}`](deployedtopology/topic-get-by-id.md)**: returns the specified deployed topology's topic

**[`GET /deployedtopology/{kafka-cluster-id}/{topology-id}/application`](deployedtopology/applications-get-by-id.md)**: returns the specified deployed topology's application identifiers

**[`GET /deployedtopology/{kafka-cluster-id}/{topology-id}/application/{local-application-id}`](deployedtopology/application-get-by-id.md)**: returns the specified deployed topology's application

## DeployedTopology-metrics

**[`GET /metrics/deployedtopology`](deployedtopologymetrics/get.md)**: returns all deployed topologies' metrics in all kafka-clusters

**[`GET /metrics/deployedtopology/{kafka-cluster-id}`](deployedtopologymetrics/get-by-kafka-cluster.md)**: returns all deployed topologies' metrics in the specified kafka-cluster

**[`GET /metrics/deployedtopology/{kafka-cluster-id}/{topology-id}`](deployedtopologymetrics/get-by-id.md)**: returns the specified deployed topology's metrics

## Resolved DeployedTopology

**[`GET /resolved/deployedtopology/{kafka-cluster-id}`](resolveddeployedtopology/get-by-kafka-cluster.md)**: returns all resolved deployed topologies' metrics in the specified kafka-cluster

**[`GET /resolved/deployedtopology/{kafka-cluster-id}/{topology-id}`](resolveddeployedtopology/get-by-id.md)**: returns the specified resolved deployed topology's metrics

## Admin

Only super-users can invoke the following end-points.

**[`GET /admin/plugin/permission/{user-name}`](admin/plugin-permission-get-by-user.md)**: returns all permissions for the specified user from the current permission plugin

**[`POST /admin/plugin/permission/invalidatecached`](admin/plugin-permission-invalidatecached-post.md)**: invalidates all cached permissions from the current permission plugin for all users

**[`POST /admin/plugin/permission/invalidatecached/{user-name}`](admin/plugin-permission-invalidatecached-post-by-user.md)**: invalidates all cached permissions from the current permission plugin for the specified user

**[`POST /admin/reinit`](admin/reinit-post.md)**: re-initializes kafkakewl from the persistence storage with or without purging all persisted state

**[`GET /admin/kafka/{kafka-cluster-id}/diff`](admin/diff-get.md)**: returns the difference between the desired and actual state for the specified kafka-cluster, ignoring non-kewl resources

**[`GET /admin/kafka/{kafka-cluster-id}/diff_all`](admin/diff-all-get.md)**: returns the difference between the desired and actual state for the specified kafka-cluster, not ignoring non-kewl resources
