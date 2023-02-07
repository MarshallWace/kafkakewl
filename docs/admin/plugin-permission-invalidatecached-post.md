**URL**: `/admin/plugin/permission/invalidatecached`

**Method**: `POST`

**Description**: invalidates all cached [permissions](../permission/Permission.md) from the current permission plugin for all users

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response` field is null.
