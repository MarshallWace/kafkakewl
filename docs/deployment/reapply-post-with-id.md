**URL**: `/deployment/reapply/{kafka-cluster-id}/{topology-id}`

**Method**: `POST`

**Query parameters**:
 - `dryrun=true|false`: optional, true if kafkakewl should just simulate what would happen (by default false)

**Request Body**: an optional [deployment-options](DeploymentOptions.md)

**Description**: re-applies (deploys) the specified [deployment](Deployment.md) if the current user has deploy permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response` field is null.
