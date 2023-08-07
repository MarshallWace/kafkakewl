# **kafkakewl-api-metrics** prometheus metrics

The **kafkakewl-api-metrics** service exposes the following prometheus metrics about topics and consumer groups.

### `kafkakewl_api_metrics_topic`

A gauge metric about topics. It has the following labels:
  - `kafkaCluster`: the kafka-cluster of the topic
  - `topic`: the topic name
  - `partition` the topic partition or `all` - currently it's always `all`, there are no partition-level metrics exposed.
  - `metric`: the metric name. Can be:
    - `incomingmsgrate`: the topic's produced message rate (moving average of the last 60 seconds, offsets / second)

### `kafkakewl_api_metrics_consumer`

A gauge metric about topics' consumer groups. It has the following labels:
  - `kafkaCluster`: the kafka-cluster of the topic and consumer group
  - `group`: the consumer group name
  - `topic`: the topic name
  - `partition` the topic partition or `all` - currently it's always `all`, there are no partition-level metrics exposed.
  - `metric`: the metric name. Can be:
    - `lag`: the current total lag of the consumer group for the topic (total of all partitions' lags)
    - `consumedrate`: the consumer's consumption (commit) rate (moving average of the last `consumerLagWindowSeconds`, offsets / second)
    - `consumerstatus`: the [consumer status](./deployedtopologymetrics/DeployedTopologyMetrics.md#consumergroupstatus):
      - `Ok` = 1
      - `Unknown` = 2
      - `Warning` = 3
      - `Error` = 4
      - `MaybeStopped` = 5
      - `Stopped` = 6
