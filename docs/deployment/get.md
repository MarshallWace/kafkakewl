**URL**: `/deployment`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns all [deployments](Deployment.md) that the current user has READ permission for.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployments` field contains the array of [entity-states](../EntityState.md) of [`deployments`](Deployment.md).
