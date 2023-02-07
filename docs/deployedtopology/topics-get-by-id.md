**URL**: `/deployedtopology/{kafka-cluster-id}/{topology-id}/topic`

**Method**: `GET`

**Description**: returns the specified [deployed-topology](DeployedTopology.md)'s topic identifiers if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.TopicIds` field contains the array of topic identifier `String`s.
