**URL**: `/permission`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns all [permission](Permission.md)s that belongs to the current user or ALL permissions if the current user is a super-user.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.permissions` field contains the array of [entity-states](../EntityState.md) of [`permission`](Permission.md)s.
