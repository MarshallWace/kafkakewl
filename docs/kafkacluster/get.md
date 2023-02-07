**URL**: `/kafkacluster`

**Method**: `GET`

**Query parameters**:
 - `compact=true|false`: optional, true if the results should be compacted (by default false)

**Description**: returns all [kafka-clusters](KafkaCluster.md) that the current user has READ permission for.

**Response**: either [`Failed`](../Failed.md) or [`Succeeded`](../Succeeded.md). In case of [`Succeeded`](../Succeeded.md), the `response.kafkaClusters` field contains the array of [entity-states](../EntityState.md) of [`kafka-cluster`](KafkaCluster.md)s.
