**URL**: `/metrics/deployedtopology/{kafka-cluster-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns all [deployed-topologies-metrics](DeployedTopologyMetrics.md) in the specified kafka-cluster that the current user has READ permission for.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployedTopologiesMetrics` field contains the array of [`deployed-topologies-metrics`](DeployedTopologyMetrics.md).
