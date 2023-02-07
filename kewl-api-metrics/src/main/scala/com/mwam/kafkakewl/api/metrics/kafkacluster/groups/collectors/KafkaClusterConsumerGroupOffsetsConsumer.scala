/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 *
 * Some of the decoding logic originates from
 * - https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go
 * - https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala
 * (converted to scala)
 *
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.groups.collectors

import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.groups.KafkaClusterConsumerGroupOffsetsCollector
import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupOffsetObserver, ConsumerGroupTopicPartition, KafkaClusterMetricsHelper}
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupOffset
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.KafkaUpsertConsumer
import com.mwam.kafkakewl.processor.kafkacluster.common.KafkaConnectionExtraSyntax
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, Gauge, MetricName}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.time.{Instant, ZoneOffset}
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Failure

object KafkaClusterConsumerGroupOffsetsConsumer {
  implicit class ByteBufferExtensions(bb: ByteBuffer) {
    /**
     * Reads a length-prefixed string from the byte-buffer. If the 16 bit length is -1 (0xFFFF) it considers it an empty string.
     *
     * This is how the `__consumer_offsets` topic's key and value encodes strings.
     */
    def getStringPrefixedWithLength(): String = {
      val length = bb.getShort()
      if (length == -1) {
        ""
      } else {
        val stringBytes = Array.fill(length)(0.toByte)
        bb.get(stringBytes, 0, length)
        new String(stringBytes, StandardCharsets.UTF_8)
      }
    }
  }

  /**
   * A kafka deserializer returning a ConsumerGroupTopicPartition from the `__consumer_offsets` topic's keys.
   *
   * It supports only version 0 or 1 and return null for version 2 (which means consumer group metadata messages will be filtered out).
   */
  class ConsumerOffsetsKeyDeserializer extends Deserializer[ConsumerGroupTopicPartition] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

    override def deserialize(topic: String, data: Array[Byte]): ConsumerGroupTopicPartition = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
      val version = bb.getShort()
      version match {
        case 0 | 1 =>
          val consumerGroup = bb.getStringPrefixedWithLength()
          val topic = bb.getStringPrefixedWithLength()
          val partition = bb.getInt()
          ConsumerGroupTopicPartition(consumerGroup, topic, partition)

        case 2 =>
          // version 2 is about consumer group metadata, which we ignore and filter out completely
          null
        case _ =>
          sys.error(s"Unsupported key version: $version. Check the code here: https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go#L388 or here: https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1144")
      }
    }

    override def close(): Unit = ()
  }

  /**
   * A kafka deserializer returning a ConsumerGroupOffset from the `__consumer_offsets` topic's values.
   *
   * It can only be used if the key's version is 0 or 1, otherwise the format of this is different (the value of consumer group metadata messages).
   */
  class ConsumerOffsetsValueDeserializer extends Deserializer[ConsumerGroupOffset] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

    override def deserialize(topic: String, data: Array[Byte]): ConsumerGroupOffset = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
      val version = bb.getShort()
      val (offset, metadata, timestamp) = version match {
        case 0 | 1 =>
          val offset = bb.getLong()
          val metadata = bb.getStringPrefixedWithLength()
          val timestamp = bb.getLong()
          (offset, metadata, timestamp)
        case 3 =>
          val offset = bb.getLong()
          val leaderEpoch = bb.getInt() // this is not needed, but we still need to skip it
          val metadata = bb.getStringPrefixedWithLength()
          val timestamp = bb.getLong()
          (offset, metadata, timestamp)
        case _ =>
          sys.error(s"Unsupported value version: $version. Check the code here: https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go#L465 or here: https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1163")
      }
      val offsetDateTime = Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)
      ConsumerGroupOffset(offset, metadata, offsetDateTime)
    }

    override def close(): Unit = ()
  }
}

class KafkaClusterConsumerGroupOffsetsConsumer(
  helper: KafkaClusterMetricsHelper,
  observer: ConsumerGroupOffsetObserver,
  enabled: Boolean,
  noOfThreadsForInitialLoad: Int = 1,
  noOfThreadsForLiveConsumption: Int = 1,
  compactionInterval: FiniteDuration = 1.seconds,
  excludeConsumerGroups: Seq[FlexibleName] = Seq.empty,
  consumerGroup: Option[String] = None,
  consumerOffsetsTopicName: String = "__consumer_offsets",
)
  extends KafkaClusterConsumerGroupOffsetsCollector
    with DefaultInstrumented
    with KafkaConnectionExtraSyntax
    with ExecutorContextFactory
    with LazyLogging
    with MdcUtils
{
  import KafkaClusterConsumerGroupOffsetsConsumer._

  implicit val consumerOffsetsKeyDeserializer: ConsumerOffsetsKeyDeserializer = new ConsumerOffsetsKeyDeserializer()
  implicit val consumerOffsetsValueDeserializer: ConsumerOffsetsValueDeserializer = new ConsumerOffsetsValueDeserializer()

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.source.consumergroup")

  private val kafkaClusterId = helper.kafkaClusterId

  // The initial value is Long.MaxValue so that it reports lag until the initial load finishes and the live consumption reduces the lag to near zero.
  // This way we can alert when the initial load takes too long
  @volatile private var totalLag = Long.MaxValue

  private val lagGaugeName = s"$kafkaClusterId:totallag"
  private val lagGauge: Gauge[Long] = {
    if (metrics.gaugeExists(lagGaugeName)) {
      metrics.removeGauge(lagGaugeName)
    }
    // yes, in theory there still can be concurrent calls creating the same gauge at the same time, but it shouldn't happen (these are per kafka-cluster, and we can only have one of this consumer per kafka-cluster)
    metrics.gauge(lagGaugeName) { totalLag }
  }

  private val threadPoolExecutorService = createThreadPoolExecutorService(
    math.max(noOfThreadsForInitialLoad, noOfThreadsForLiveConsumption) + 1,
    s"kafkacluster-consumeroffset-consumer-thread-$kafkaClusterId-"
  )

  implicit val executionContext: ExecutionContext = decorateExecutionContextWithMDC(getCurrentMDC)(threadPoolExecutorService)

  private val shouldStop = new AtomicBoolean(false)

  private def postNotifyObserver(
    offsets: Map[ConsumerGroupTopicPartition, ConsumerGroupOffset],
    removedOffsets: Iterable[ConsumerGroupTopicPartition],
    notifyDuration: FiniteDuration
  ): Unit = {
    val noOfConsumerGroups = offsets.keys.groupBy(cgtp => cgtp.consumerGroupId).size
    val noOfConsumerGroupTopics = offsets.keys.groupBy(cgtp => (cgtp.consumerGroupId, cgtp.topicPartition.topic)).size
    val noOfConsumerGroupTopicPartitions = offsets.keys.groupBy(cgtp => (cgtp.consumerGroupId, cgtp.topicPartition.topic, cgtp.topicPartition.partition)).size
    val noOfRemovedConsumerGroups = removedOffsets.groupBy(cgtp => cgtp.consumerGroupId).size
    val noOfRemovedConsumerGroupTopics = removedOffsets.groupBy(cgtp => (cgtp.consumerGroupId, cgtp.topicPartition.topic)).size
    val noOfRemovedConsumerGroupTopicPartitions = removedOffsets.groupBy(cgtp => (cgtp.consumerGroupId, cgtp.topicPartition.topic, cgtp.topicPartition.partition)).size

    metrics.timer(s"$kafkaClusterId:notifyobservers").update(notifyDuration)
    logger.info(s"consumeConsumerOffsets(${if (enabled) "enabled" else "disabled"}): received $noOfConsumerGroups consumer groups' offsets ($noOfConsumerGroupTopics / $noOfConsumerGroupTopicPartitions), deleted $noOfRemovedConsumerGroups consumer groups ($noOfRemovedConsumerGroupTopics / $noOfRemovedConsumerGroupTopicPartitions), notified observers in ${notifyDuration.toMillis} ms")
  }

  private def shouldBeIncluded(consumerGroup: String): Boolean = !excludeConsumerGroups.exists(_.doesMatch(consumerGroup))
  private def shouldBeIncluded(consumerGroupTopicPartition: ConsumerGroupTopicPartition): Boolean = shouldBeIncluded(consumerGroupTopicPartition.consumerGroupId)

  private def failFastWith(throwable: Throwable): Unit = {
    ApplicationMetrics.errorCounter.inc()
    logger.error(s"TERMINATING due to a failure while consuming the $consumerOffsetsTopicName topic", throwable)
    failFast(1)
  }

  logger.info(s"kafka-cluster = $kafkaClusterId")
  logger.info(s"enabled = $enabled")
  logger.info(s"noOfThreadsForInitialLoad = $noOfThreadsForInitialLoad")
  logger.info(s"noOfThreadsForLiveConsumption = $noOfThreadsForLiveConsumption")
  logger.info(s"compactionInterval = $compactionInterval")
  logger.info(s"consumerGroup = ${consumerGroup.getOrElse("n/a")}")
  logger.info(s"consumerOffsetsTopicName = $consumerOffsetsTopicName")

  private val consumeFuture = if (!enabled) {
    Future.successful(())
  } else {
    for {
      // consuming the consumer-offsets topic until the end, compacting, getting the latest values for keys
      latestConsumerOffsets <- KafkaUpsertConsumer.loadLatestKeyValuesOfTopic[ConsumerGroupTopicPartition, ConsumerGroupOffset](
        helper.connection.withConfig(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520"), // to speed up the initial load
        consumerOffsetsTopicName,
        noOfThreadsForInitialLoad,
        logger.some,
        clientIdPrefix = s"consumeroffset-consumer-initial-load-$kafkaClusterId-",
        consumerGroup
      )

      // filtering out the excluded consumer groups
      latestConsumerOffsetsWithExclusions = latestConsumerOffsets.filterKeys(shouldBeIncluded)

      _ = {
        // notifying the observer about the initial load of consumer group offsets
        val (_, notifyDuration) = durationOf {
          observer.updateAllConsumerGroupOffsets(kafkaClusterId, latestConsumerOffsetsWithExclusions.latestKeyValuesWithoutTombstones)
        }
        postNotifyObserver(latestConsumerOffsetsWithExclusions.latestKeyValuesWithoutTombstones, Set.empty, notifyDuration)
      }

      // live-consuming the consumer-offsets topic with compaction at every compactionInterval
      _ <- KafkaUpsertConsumer.consumeTopicPartitions[ConsumerGroupTopicPartition, ConsumerGroupOffset](
        helper.connection,
        latestConsumerOffsetsWithExclusions.offsets,
        (latestConsumerOffsets, lags) => {
          // filtering out the excluded consumer groups
          val latestConsumerOffsetsWithExclusions = latestConsumerOffsets.filterKeys(shouldBeIncluded)
          if (latestConsumerOffsetsWithExclusions.latestKeyValues.nonEmpty) {
            val (_, notifyDuration) = durationOf {
              observer.updateConsumerGroupOffsets(kafkaClusterId, latestConsumerOffsetsWithExclusions.latestKeyValuesWithoutTombstones, latestConsumerOffsetsWithExclusions.latestKeysWithTombstones)
            }
            postNotifyObserver(latestConsumerOffsetsWithExclusions.latestKeyValuesWithoutTombstones, latestConsumerOffsetsWithExclusions.latestKeysWithTombstones, notifyDuration)
          }
          // even if we haven't received any new key-values, we still deal with the lag
          val topicPartitionsWithNoLag = lags.collect { case (tp, None) => tp }.toSeq
          if (topicPartitionsWithNoLag.nonEmpty) {
            logger.warn(s"no lag for ${topicPartitionsWithNoLag.toPrettyString}")
          }
          totalLag = lags.collect { case (_, Some(lag)) => lag }.sum
          metrics.meter(s"$kafkaClusterId:receivedconsumergroupoffsets").mark()
        },
        (topicPartitions, throwable) => {
          logger.warn(s"failed to get the end-offsets for ${topicPartitions.toPrettyString}", throwable)
          metrics.meter(s"$kafkaClusterId:endoffsetsfailure").mark()
        },
        failFastWith,
        shouldStop,
        noOfThreadsForLiveConsumption,
        compactionInterval,
        logger.some,
        clientIdPrefix = s"consumeroffset-consumer-live-$kafkaClusterId-",
        consumerGroup
      )
    } yield ()
  }

  consumeFuture.onComplete {
    case Failure(throwable) => failFastWith(throwable)
    case _ =>
  }

  override def stop(): Unit = {
    shouldStop.set(true)
    Await.result(consumeFuture, Duration.Inf)
    metrics.removeGauge(lagGaugeName)
  }
}
