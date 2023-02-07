**URL**: `/deployedtopology/{kafka-cluster-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns all [deployed-topologies](DeployedTopology.md) in the specified kafka-cluster that the current user has READ permission for.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.deployedTopologies` field contains the array of [entity-states](../EntityState.md) of [`deployed-topologies`](DeployedTopology.md).
