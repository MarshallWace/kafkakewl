**URL**: `/topology/{topology-id}/{topology-version}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [topology](Topology.md) version if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.topology` field contains the [entity-state](../EntityState.md) of a [`topology`](Topology.md).
