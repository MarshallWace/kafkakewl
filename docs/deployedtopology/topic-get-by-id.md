**URL**: `/deployedtopology/{kafka-cluster-id}/{topology-id}/topic/{local-topic-id}`

**Method**: `GET`

**Description**: returns the specified [deployed-topic](DeployedTopic.md)

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.DeployedTopic` field contains the [deployed-topic](DeployedTopic.md).
