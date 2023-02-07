**URL**: `/deployment/{kafka-cluster-id}/{topology-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [deployment](Deployment.md) if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployment` field contains the [entity-state](../EntityState.md) of a [`deployment`](Deployment.md).
