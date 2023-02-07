**URL**: `/admin/plugin/permission/{user-name}`

**Method**: `GET`

**Description**: returns all [permissions](../permission/Permission.md) for the specified user from the current permission plugin

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.permissions` field contains the array of [entity-states](../EntityState.md) of [`permission`](Permission.md)s.
