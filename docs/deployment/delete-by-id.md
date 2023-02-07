**URL**: `/deployment/{kafka-cluster-id}/{topology-id}`

**Method**: `DELETE`

**Query parameters**:
 - `dryrun=true|false`: optional, true if kafkakewl should just simulate what would happen (by default false)

**Description**: deletes the specified [deployment](Deployment.md) if the current user has write permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response` field is null.
