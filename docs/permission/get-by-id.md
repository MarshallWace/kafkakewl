**URL**: `/permission/{permission-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [permission](Permission.md) if the current user is a super-user or the permission is hers.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.permission` field contains the [entity-state](../EntityState.md) of a [`permission`](Permission.md).
