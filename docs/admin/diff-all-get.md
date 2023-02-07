**URL**: `/admin/kafka/{kafka-cluster-id}/diff_all`

**Method**: `GET`

**Description**: returns the difference between the desired and actual state for the specified kafka-cluster, not ignoring non-kewl resources

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.DiffResult` field contains the [`diff-results`](DiffResult.md).
