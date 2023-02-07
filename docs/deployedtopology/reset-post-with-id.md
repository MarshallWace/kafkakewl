**URL**: `/deployedtopology/{kafka-cluster-id}/{topology-id}/reset`

**Method**: `POST`

**Query parameters**:
 - `dryrun=true|false`: optional, true if kafkakewl should just simulate what would happen (by default false)

**Request Body**: the [reset-options](ResetOptions.md)

**Description**: resets an application in the specified deployed topology.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.ApplicationOffsetsReset` field contains the resulting [application-offsets-reset](ApplicationOffsetsReset.md).
