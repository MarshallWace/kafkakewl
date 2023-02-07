**URL**: `/admin/kafka/{kafka-cluster-id}/diff`

**Method**: `GET`

**Description**: returns the difference between the desired and actual state for the specified kafka-cluster, ignoring non-kewl resources

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.permissions` field contains the array of [entity-states](../EntityState.md) of [`permission`](Permission.md)s.
