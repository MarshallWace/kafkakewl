# **kafkakewl-api-metrics** Configuration

The **kafkakewl-api-metrics** service is configured with the usual [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) config files ([application.conf](../kewl-api-metrics/src/main/resources/application.conf)), exposing lots of environment variables to make it easier to tweak certain settings.

It's also possible to override parts of the main `application.conf` config files with the following optional files in the source root directory (or at runtime the current directory):

`.kafkakewl-api-metrics-overrides.conf`

Overriding is useful when some sections need to tbe completely custom and environment variables are cumbersome or not possible to use.

# Important configurable components

## Persistence

The **kafkakewl-api-metrics** doesn't persist anything, but it can consume the **kafkakewl-api**'s change-log: the `kewl.changelog` topic. The main reason is that it needs to know the registered kafka-clusters and gather metrics about them and also, it needs a few properties from the topologies so that it can tweak its monitoring.

It's possible to run it in standalone mode though: by just hard-coding a set of kafka-clusters to monitor. In this case however it won't use the above mentioned properties from topologies, so it'll be somewhat limited.

# The most important configuration options

`KAFKAKEWL_API_METRICS_HTTP_PORT`: the http port of the REST API, by default 8090.

`KAFKAKEWL_API_METRICS_HTTP_ALLOWED_ORIGINS`: an optional comma separated list of allowed origins for CORS, by default empty.

`KAFKAKEWL_API_METRICS_CHANGELOG_ENABLED`: optional, indicates whether the **kafkakewl-api-metrics** should consume the `kewl.changelog` topic or not, by default true.

If it's true and you want to customize the kafka client config too, you can override the default `application.conf` with `.kafkakewl-api-metrics-overrides.conf`:

```
kafkakewl-api-metrics {
  changelog-store {
    kafka-cluster {
      brokers = "kafka-broker-1:9092"
      kafka-client-config {
        whatever-kafka-config: "abc"
      }
    }
    enabled = false
  }
}
```

Look at the default [application.conf](../kewl-api-metrics/src/main/resources/application.conf) and you can see what else can be overriden this way (everything really).

If it's set to false, you'll need to enable the `kafkakewl-api-metrics.additional-kafka-clusters` section and configure the kafka clusters to be monitored manually, for details see below.

`KAFKAKEWL_API_METRICS_BROKERS`: the comma separated list of kafka brokers where the `kewl.changelog` topic will be consumed from.

Needs to be set only if `KAFKAKEWL_API_METRICS_CHANGELOG_ENABLED` is true.

`KAFKAKEWL_API_METRICS_SECURITY_PROTOCOL`: the optional security protocol of the previous kafka brokers.

`KAFKAKEWL_API_METRICS_JAAS_CONFIG`: the optional jaas config for the previous kafka brokers

`KAFKAKEWL_API_METRICS_ADDITIONAL_KAFKA_CLUSTERS_ENABLED`: optional, indicates whether the **kafkakewl-api-metrics** should monitor the kafka-clusters in the `kafkakewl-api-metrics.additional-kafka-clusters` instead of consuming the `kewl.changelog` topic.

If it's true you can override the default `application.conf` with `.kafkakewl-api-metrics-overrides.conf` to specify the clusters you want to monitor:

```
kafkakewl-api-metrics {
  additional-kafka-clusters {
    clusters = [
      { id = "kafka-cluster-id", brokers = "kafka-broker-1:9092", security-protocol = "PLAINTEXT", kafka-client-config {}, jaas-config = "The-jaas-config" }
    ]
    enabled = true
  }
}
```

Look at the default [application.conf](../kewl-api-metrics/src/main/resources/application.conf) and you can see what else can be overriden this way (everything really).

# The rest of the configuration options

## Environment Variables:

`WAIT_FOR_FILE_AT_STARTUP`: optional, before it starts, waits for this file to exist.

It's useful if e.g. you want to make sure that the kerberos token cache file exists.

`LOGBACK_ELASTIC_KERBEROS_PRINCIPAL`: the optional kerberos principal for the elastic-search logging.

`LOGBACK_ELASTIC_KERBEROS_KEYTAB`: the optional kerberos keytab path for the elastic-search logging.

`KAFKAKEWL_API_METRICS_POLLING_POOL_SIZE`: the optional pool-size for the metrics polling thread-pool, by default 32.

`KAFKAKEWL_API_METRICS_TOPIC_INFO_POLLER_DELAYMS`: the optional delay in milliseconds between topic metrics polling, by default 2000.

`KAFKAKEWL_API_METRICS_CONSUMER_GROUP_OFFSETS_POLLER_DELAYMS`: the optional delay in milliseconds between the consumer group metrics polling, by default 2000.

`KAFKAKEWL_API_METRICS_CONSUMER_GROUP_OFFSETS_SOURCE`: the optional source of consumer group offsets, by default `consumer` (the other accepted value is `poller`) 

`KAFKAKEWL_API_METRICS_CONSUMER_GROUP_OFFSETS_CONSUMER_CONSUMER_GROUP`: the optional consumer group name for consuming the `__consumer_offsetse` topic if the consumer group offsets source above is set to `consumer`.  