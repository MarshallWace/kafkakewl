**URL**: `/resolved/deployedtopology/{kafka-cluster-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Request Body**: an optional array of [flexible names](../FlexibleName.md) filtering the resolved topologies to .return

**Description**: returns all [resolved deployed-topologies](ResolvedDeployedTopology.md) in the specified kafka-cluster that the current user has READ permission for.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployedTopologies` field contains the map of [`resolved deployed-topologies`](ResolvedDeployedTopology.md) by topology id.
