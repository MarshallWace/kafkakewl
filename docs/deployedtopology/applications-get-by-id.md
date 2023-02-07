**URL**: `/deployedtopology/{kafka-cluster-id}/{topology-id}/application`

**Method**: `GET`

**Description**: returns the specified [deployed-topology](DeployedTopology.md)'s application identifiers if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.ApplicationIds` field contains the array of application identifier `String`s.
