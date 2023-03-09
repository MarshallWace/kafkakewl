**URL**: `/permission/{permission-id}`

**Method**: `DELETE`

**Query parameters**:
 - `dryrun=true|false`: optional, true if kafkakewl should just simulate what would happen (by default false)

**Request Body**: a [permission](Permission.md)

**Description**: deletes the [permission](Permission.md) that matches the request body, if the current user is a super-user.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response` field is null.