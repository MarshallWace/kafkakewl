**URL**: `/metrics/deployedtopology/{kafka-cluster-id}/{topology-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [deployed-topology-metrics](DeployedTopologyMetrics.md) if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployedTopologyMetrics` field contains the [`deployed-topology-metrics`](DeployedTopologyMetrics.md).
