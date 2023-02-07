/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics

import com.mwam.kafkakewl.kafka.utils._
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.ConsumerGroupOffsetMode
import com.mwam.kafkakewl.common.ConfigProvider
import com.mwam.kafkakewl.common.http.HttpConfigProvider
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration._

trait HttpServerAppConfigProvider extends ExecutorContextFactory with HttpConfigProvider with ConfigProvider {
  this: LazyLogging =>

  implicit val executionContext: ExecutionContextExecutor

  val config: Config = loadConfigWithDefaultsAndOverrides(prefix = ".kafkakewl-api-metrics")
  val kafkaKewlApiMetricsConfig: Config = config.getConfig("kafkakewl-api-metrics")
  val configForHttp: Config = kafkaKewlApiMetricsConfig

  config.getStringOrNoneIfEmpty("wait-for-file-at-startup.name").foreach { WaitForFile.filePath(_) }

  val kafkaChangeLogStoreConnection: KafkaConnection = kafkaKewlApiMetricsConfig.kafkaConnection("changelog-store.kafka-cluster")
    .getOrElse(throw new RuntimeException("changelog-store.kafka-cluster must be specified"))
  val kafkaChangeLogStoreEnabled: Boolean = kafkaKewlApiMetricsConfig.getBooleanOrNone("changelog-store.enabled").getOrElse(false)

  val additionalKafkaClusters: Map[KafkaClusterEntityId, KafkaCluster] = kafkaKewlApiMetricsConfig.getConfigList("additional-kafka-clusters.clusters").asScala
    .flatMap(c =>
      for {
        id <- c.getStringOrNone("id")
        info <- c.kafkaConnectionInfo("")
      } yield {
        val kafkaClusterId = KafkaClusterEntityId(id)
        (kafkaClusterId, KafkaCluster(kafkaClusterId, info.brokers, info.securityProtocol))
      }
    )
    .toMap
  val additionalKafkaClustersEnabled: Boolean = kafkaKewlApiMetricsConfig.getBooleanOrNone("additional-kafka-clusters.enabled").getOrElse(true)

  if (additionalKafkaClustersEnabled == kafkaChangeLogStoreEnabled) throw new RuntimeException(s"it's not possible to enable/disable both changelog-store and additional-kafka-cluster")

  def kafkaConnectionFrom(kafkaCluster: KafkaCluster, jaasConfig: Option[String] = kafkaChangeLogStoreConnection.jaasConfig): KafkaConnection =
    KafkaConnection(KafkaConnectionInfo(kafkaCluster.brokers, kafkaCluster.securityProtocol, kafkaCluster.kafkaClientConfig), jaasConfig)

  val (pollingExecutionContext, pollingParallelism) = createExecutorContext(
    kafkaKewlApiMetricsConfig.getConfig("polling"),
    namePrefix = "kafkacluster-polling-thread-"
  )

  val delayMillisBetweenTopicInfoPolls: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("topic-info-poller.delay-millis").getOrElse(2000)
  val disabledKafkaClusterIdsForTopicInfoPolls: Set[KafkaClusterEntityId] = kafkaKewlApiMetricsConfig.getStringOrNone("topic-info-poller.disabled-kafka-clusters").getOrElse("")
    .split(',').map(_.trim).filter(_.nonEmpty).map(KafkaClusterEntityId).toSet

  val delayMillisBetweenConsumerGroupOffsetsPolls: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("consumer-group-offsets.poller.delay-millis").getOrElse(2000)
  val disabledKafkaClusterIdsForConsumerGroupOffsets: Set[KafkaClusterEntityId] = kafkaKewlApiMetricsConfig.getStringOrNone("consumer-group-offsets.disabled-kafka-clusters").getOrElse("")
    .split(',').map(_.trim).filter(_.nonEmpty).map(KafkaClusterEntityId).toSet
  val excludedConsumerGroupRegexesForConsumerGroupOffsets: Seq[FlexibleName] = kafkaKewlApiMetricsConfig.getStringOrNone("consumer-group-offsets.excluded-consumer-groups-regexes").getOrElse("")
    .split(',').map(_.trim).filter(_.nonEmpty).map(FlexibleName.Regex)

  val compactionDurationForConsumerGroupConsumption: FiniteDuration = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("consumer-group-offsets.consumer.compaction-millis").getOrElse(1000).millis
  val threadsForInitialLoadForConsumerGroupConsumption: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("consumer-group-offsets.consumer.threads-for-initial-load").getOrElse(5)
  val threadsForLiveConsumptionForConsumerGroupConsumption: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("consumer-group-offsets.consumer.threads-for-live-consumption").getOrElse(1)
  val consumerGroupForConsumerGroupConsumption: Option[String] = kafkaKewlApiMetricsConfig.getStringOrNoneIfEmpty("consumer-group-offsets.consumer.consumer-group")
  val topicForConsumerGroupConsumption: String = kafkaKewlApiMetricsConfig.getStringOrNoneIfEmpty("consumer-group-offsets.consumer.consumer-offsets-topic").getOrElse("__consumer_offsets")

  val consumerGroupOffsetMode: ConsumerGroupOffsetMode = ConsumerGroupOffsetMode(kafkaKewlApiMetricsConfig.getStringOrNoneIfEmpty("consumer-group-offsets.source").getOrElse("poller"))

  val disableKafkaClusterIds: Set[KafkaClusterEntityId] = disabledKafkaClusterIdsForTopicInfoPolls union disabledKafkaClusterIdsForConsumerGroupOffsets

  val defaultLagWindowMinutes: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("lag-evaluating.default-window-minutes").getOrElse(10)
  val defaultLagWindowFillFactorMinutes: Double = kafkaKewlApiMetricsConfig.getDoubleOrNoneIfEmpty("lag-evaluating.default-window-fill-factor").getOrElse(0.5)

  val consumeAfterConsumerGroupOffsetEnabled: Boolean = kafkaKewlApiMetricsConfig.getBooleanOrNone("lag-evaluating.consume-after-consumer-group-offset.enabled").getOrElse(false)
  val consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long = kafkaKewlApiMetricsConfig.getLongOrNoneIfEmpty("lag-evaluating.consume-after-consumer-group-offset.max-topic-partition-lag-to-fix").getOrElse(100)
  val consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName] = kafkaKewlApiMetricsConfig.getStringOrNone("lag-evaluating.consume-after-consumer-group-offset.excluded-topic-regexes").getOrElse("")
    .split(',').map(_.trim).filter(_.nonEmpty).map(FlexibleName.Regex)
  val consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName] = kafkaKewlApiMetricsConfig.getStringOrNone("lag-evaluating.consume-after-consumer-group-offset.excluded-consumer-group-regexes").getOrElse("")
    .split(',').map(_.trim).filter(_.nonEmpty).map(FlexibleName.Regex)
  val consumeAfterConsumerGroupOffsetNumberOfWorkers: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("lag-evaluating.consume-after-consumer-group-offset.number-of-workers").getOrElse(10)
  val consumeAfterConsumerGroupOffsetPollDurationMillis: Int = kafkaKewlApiMetricsConfig.getIntOrNoneIfEmpty("lag-evaluating.consume-after-consumer-group-offset.poll-duration-millis").getOrElse(100)
}
