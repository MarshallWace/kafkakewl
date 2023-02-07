**URL**: `/resolved/deployedtopology/{kafka-cluster-id}/{topology-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [resolved deployed-topology](ResolvedDeployedTopology.md) if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployedTopologies` field contains the map of [`resolved deployed-topologies`](ResolvedDeployedTopology.md) by topology id.

Note that here the response map contains only a single key-value.
