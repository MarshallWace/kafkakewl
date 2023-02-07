**URL**: `/kafkacluster/{kafka-cluster-id}`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns the specified [kafka-cluster](KafkaCluster.md) if the current user has READ permission for it.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.kafkaCluster` field contains the [entity-state](../EntityState.md) of a [`kafka-cluster`](KafkaCluster.md).
