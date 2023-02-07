/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers

import java.time.{Clock, OffsetDateTime}

import cats.syntax.either._
import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.{KafkaClusterObserver, TopicInfoObserver}
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

import scala.collection.JavaConverters._
import scala.collection.{SortedMap, concurrent, mutable}
import scala.concurrent.duration._

trait TopicMetricsCache {
  def getKafkaClusterIds: CacheResult[Iterable[KafkaClusterEntityId]]
  def getTopics(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]]
  def getTopicMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String): CacheResult[TopicMetricsCache.MetricsWithTimestamp]
}

object TopicMetricsCache {
  final case class MetricsWithTimestamp(
    topicMetrics: Option[KafkaTopic.Metrics],
    partitionMetrics: SortedMap[Int, KafkaTopic.Metrics]
  )
  object MetricsWithTimestamp {
    def apply(
      duration: Duration,
      newMessages: SortedMap[Int, (Long, OffsetDateTime)]
    ): MetricsWithTimestamp = {
      // the topic metrics' timestamp will be the maximum partition timestamp
      val maxLastOffsetTimestampUtc = if (newMessages.isEmpty) None else newMessages.values.map { case (_, lastOffsetTimestampUtc) => lastOffsetTimestampUtc }.max.some
      new MetricsWithTimestamp(
        // topic metrics
        maxLastOffsetTimestampUtc.map(ts =>
          KafkaTopic.Metrics(
            duration,
            newMessages.values.map { case (numberOfNewMessages, _) => numberOfNewMessages }.sum,
            ts
          )
        ),
        // partition metrics
        newMessages.mapValues { case (numberOfNewMessages, lastOffsetTimestampUtc) => KafkaTopic.Metrics(duration, numberOfNewMessages, lastOffsetTimestampUtc) }
      )
    }
  }
}

object TopicMetricsCacheImpl {
  final case class KafkaTopicInfoWithTimestamp(
    timestampUtc: OffsetDateTime,
    topicInfo: KafkaTopic.Info,
    lastOffsetTimestampsUtc: SortedMap[Int, OffsetDateTime]
  )

  object KafkaTopicInfoWithTimestamp {
    def apply(
      previous: Option[KafkaTopicInfoWithTimestamp],
      timestampUtc: OffsetDateTime,
      topicInfo: KafkaTopic.Info
    ): KafkaTopicInfoWithTimestamp = {
      new KafkaTopicInfoWithTimestamp(
        timestampUtc,
        topicInfo,
        topicInfo.partitions.map {
          case (partition, partitionInfo) =>
            val currentLastOffsetTimestampUtc = timestampUtc
            val currentHighOffset = partitionInfo.highOffset
            val previousLastOffsetTimestampUtc = previous.flatMap(_.lastOffsetTimestampsUtc.get(partition)).getOrElse(currentLastOffsetTimestampUtc)
            val previousHighOffset = previous.flatMap(_.topicInfo.partitions.get(partition).map(_.highOffset)).getOrElse(currentHighOffset)
            // using the previous timestamp if the new offset is not higher than the previous
            (partition, if (currentHighOffset > previousHighOffset) currentLastOffsetTimestampUtc else previousLastOffsetTimestampUtc)
        }
      )
    }
  }
}

class TopicMetricsCacheImpl(windowDuration: Duration) extends TopicMetricsCache
  with LazyLogging
  with KafkaClusterObserver
  with TopicInfoObserver
  with DefaultInstrumented {

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.topic")

  import TopicMetricsCache._
  import TopicMetricsCacheImpl._

  private val topicInfoQueuesByKafkaClusterId = concurrent.TrieMap[KafkaClusterEntityId, mutable.SortedMap[String, mutable.Queue[KafkaTopicInfoWithTimestamp]]]()
  private val topicMetricsByKafkaClusterId = concurrent.TrieMap[KafkaClusterEntityId, SortedMap[String, MetricsWithTimestamp]]()

  private def createMetricsWithTimestamp(topicInfoQueue: Seq[KafkaTopicInfoWithTimestamp]): Option[MetricsWithTimestamp] = {
    for {
      first <- topicInfoQueue.headOption
      last <- topicInfoQueue.lastOption
      newMessages <-
        if (topicInfoQueue.size > 1)
          Some(
            (first.topicInfo.partitions.keySet intersect last.topicInfo.partitions.keySet)
              .map { p =>
                val numberOfNewMessages = last.topicInfo.partitions(p).highOffset - first.topicInfo.partitions(p).highOffset
                val lastOffsetTimestampUtc = last.lastOffsetTimestampsUtc.getOrElse(p, last.timestampUtc)
                (p, (numberOfNewMessages, lastOffsetTimestampUtc))
              }
              .toSortedMap
          )
        else
          None
    } yield MetricsWithTimestamp(
      Duration(java.time.Duration.between(first.timestampUtc, last.timestampUtc).toMillis, MILLISECONDS),
      newMessages
    )
  }

  private def getCurrentTopicMsgCountForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String): Option[Long] =
    topicInfoQueuesByKafkaClusterId.get(kafkaClusterId)
      .flatMap(_.get(topic))
      .flatMap(_.lastOption)
      .map(_.topicInfo.partitions.values.map(_.numberOfOffsets).sum)

  private def getCurrentTopicIncomingMsgRateForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String): Option[Double] =
    topicMetricsByKafkaClusterId.get(kafkaClusterId)
      .flatMap(_.get(topic))
      .flatMap(_.topicMetrics)
      .map(_.incomingMessagesPerSecond)

  private def getCurrentPartitionMsgCountForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String, partition: Int): Option[Long] =
    topicInfoQueuesByKafkaClusterId.get(kafkaClusterId)
      .flatMap(_.get(topic))
      .flatMap(_.lastOption)
      .flatMap(_.topicInfo.partitions.get(partition))
      .map(_.numberOfOffsets)

  private def getCurrentPartitionIncomingMsgRateForGaugeMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String, partition: Int): Option[Double] =
    topicMetricsByKafkaClusterId.get(kafkaClusterId)
      .flatMap(_.get(topic))
      .flatMap(_.partitionMetrics.get(partition))
      .map(_.incomingMessagesPerSecond)

  private def updateTopicAndPartitionMetrics(kafkaClusterId: KafkaClusterEntityId, newTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit = {
    val incomingMsgRate = "incomingmsgrate"
    //val msgCount = "msgcount"
    def partitionGaugeNameOf(topic: String, partition: Int, gaugeName: String) = s"$kafkaClusterId:$gaugeName:$topic:$partition"
    def topicGaugeNameOf(topic: String, gaugeName: String) = s"$kafkaClusterId:$gaugeName:$topic:all"
    def getGaugeFullNamesOf(names: String*): Set[String] = {
      val startsWithNames = names.map(name => metrics.metricFullName(metricName = s"$kafkaClusterId:$name:"))
      metricRegistry.getGauges((name, metric) => startsWithNames.exists(name.startsWith)).asScala.keys.toSet
    }

    val existingGaugeFullNames = getGaugeFullNamesOf(/*msgCount, */incomingMsgRate)

    // for not we don't publish per-partition data, it's too many gauges
    val newPartitionGaugeFuncs = Map.empty
//    val newPartitionGaugeFuncs = newTopicInfos
//      .flatMap { case (topic, topicInfo) =>
//        topicInfo.partitions.keys
//          .flatMap { partition => Seq[CreateGaugeFuncWithName](
//            metrics.createGaugeFuncWithName(partitionGaugeNameOf(topic, partition, msgCount), defaultValue = 0L)(
//              getCurrentPartitionMsgCountForGaugeMetrics(kafkaClusterId, topic, partition)
//            ),
//            metrics.createGaugeFuncWithName(partitionGaugeNameOf(topic, partition, incomingMsgRate), defaultValue = 0.0)(
//              getCurrentPartitionIncomingMsgRateForGaugeMetrics(kafkaClusterId, topic, partition)
//            )
//          )}
//      }
//      .toMap

    val newTopicGaugeFuncs = newTopicInfos
      .keys
      .flatMap { topic => Seq[CreateGaugeFuncWithName](
//        metrics.createGaugeFuncWithName(topicGaugeNameOf(topic, msgCount), defaultValue = 0L)(
//          getCurrentTopicMsgCountForGaugeMetrics(kafkaClusterId, topic)
//        ),
        metrics.createGaugeFuncWithName(topicGaugeNameOf(topic, incomingMsgRate), 0.0)(
          getCurrentTopicIncomingMsgRateForGaugeMetrics(kafkaClusterId, topic)
        ),
      )}
      .toMap

    val newGaugeFuncs = newPartitionGaugeFuncs ++ newTopicGaugeFuncs
    val gaugeFullNamesToAdd = newGaugeFuncs.keys.toSet diff existingGaugeFullNames
    gaugeFullNamesToAdd.foreach { gaugeFullName => newGaugeFuncs(gaugeFullName)() }
  }

  def remove(kafkaClusterId: KafkaClusterEntityId): Unit = {
    topicInfoQueuesByKafkaClusterId.remove(kafkaClusterId)
    topicMetricsByKafkaClusterId.remove(kafkaClusterId)
  }

  def updateAllTopicInfos(timestampUtc: OffsetDateTime, kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit = {
    val topicInfoQueues = topicInfoQueuesByKafkaClusterId.getOrElseUpdate(kafkaClusterId, mutable.SortedMap.empty)
    // from now on we assume the topicInfoQueues is not manipulated concurrently, but can be read concurrently

    // first remove all the topic info queues that don't exist in the current set of topic infos
    // (we assume that those topics actually don't exist hence we don't need to keep their queue)
    topicInfoQueues.keys.toSeq.foreach { topic =>
      if (!allTopicInfos.contains(topic)) topicInfoQueues.remove(topic)
    }
    // then we add or update the remaining queues
    allTopicInfos.foreach { case (topic, topicInfo) =>
      val previousTopicInfoWithTimeStamp = topicInfoQueues.get(topic).flatMap(_.lastOption)
      // the new KafkaTopicInfoWithTimestamp is calculated from the previous one and from the new topicInfos
      // each partition's lastOffsetTimestampUtc is taken from the previous if the high offset is the same
      val topicInfoWithTimeStamp = KafkaTopicInfoWithTimestamp(previousTopicInfoWithTimeStamp, timestampUtc, topicInfo)

      topicInfoQueues.get(topic) match {
        case Some(topicInfoQueue) => topicInfoQueue += topicInfoWithTimeStamp
        case None => topicInfoQueues += (topic -> mutable.Queue(topicInfoWithTimeStamp))
      }
    }
    // we dequeue all too old items in the queues
    topicInfoQueues.foreach { case (_, topicInfoQueue) =>
      while (topicInfoQueue.headOption.exists(t => timestampUtc.minusNanos(windowDuration.toNanos).isAfter(t.timestampUtc))) {
        topicInfoQueue.dequeue()
      }
    }

    // now we can re-calculate the metrics
    val allTopicMetrics = topicInfoQueues
      .flatMap { case (topic, topicQueue) => createMetricsWithTimestamp(topicQueue).map { mwt => (topic, mwt) } }
      .toSortedMap

    // publish it as metrics
    updateTopicAndPartitionMetrics(kafkaClusterId, allTopicInfos)

    // atomically updating the metrics for this kafka-cluster
    topicMetricsByKafkaClusterId.update(kafkaClusterId, allTopicMetrics)
  }

  def updateAllTopicInfos(kafkaClusterId: KafkaClusterEntityId, allTopicInfos: SortedMap[String, KafkaTopic.Info]): Unit =
    updateAllTopicInfos(OffsetDateTime.now(Clock.systemUTC()), kafkaClusterId, allTopicInfos)

  def getKafkaClusterIds: CacheResult[Iterable[KafkaClusterEntityId]] = topicMetricsByKafkaClusterId.keys.toSeq.sortBy(_.id).asRight

  def getTopics(kafkaClusterId: KafkaClusterEntityId): CacheResult[Iterable[String]] = {
    for {
      topicsMetrics <- topicInfoQueuesByKafkaClusterId.get(kafkaClusterId).toRight(CacheError.NoTopicMetricsForKafkaCluster)
    } yield topicsMetrics.keys
  }

  def getTopicMetrics(kafkaClusterId: KafkaClusterEntityId, topic: String): CacheResult[MetricsWithTimestamp] = {
    for {
      topicsMetrics <- topicMetricsByKafkaClusterId.get(kafkaClusterId).toRight(CacheError.NoTopicMetricsForKafkaCluster)
      metrics <- topicsMetrics.get(topic).toRight(CacheError.NoTopicMetricsForTopic)
    } yield metrics
  }
}
