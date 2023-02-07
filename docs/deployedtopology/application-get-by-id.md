**URL**: `/deployedtopology/{kafka-cluster-id}/{topology-id}/application/{local-application-id}`

**Method**: `GET`

**Description**: returns the specified [deployed-application](DeployedApplication.md)

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.DeployedApplication` field contains the [deployed-application](DeployedApplication.md).
