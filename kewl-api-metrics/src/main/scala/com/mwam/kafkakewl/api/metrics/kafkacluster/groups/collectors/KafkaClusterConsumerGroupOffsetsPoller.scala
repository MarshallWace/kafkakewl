/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.groups.collectors

import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicBoolean
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.api.metrics.kafkacluster.groups.KafkaClusterConsumerGroupOffsetsCollector
import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupOffsetObserver, ConsumerGroupTopicPartition, KafkaClusterMetricsHelper}
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupOffset
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

import scala.concurrent._
import scala.concurrent.duration.{Duration, FiniteDuration}

object KafkaClusterConsumerGroupOffsetsPoller {
  final case class GetConsumerGroupOffsetsResult(
    listConsumerGroupsDuration: FiniteDuration,
    getConsumerGroupOffsetsDuration: FiniteDuration,
    errors: Map[String, WithUtcTimestamp[Throwable]],
    offsets: Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]]
  )
}

class KafkaClusterConsumerGroupOffsetsPoller(
  helper: KafkaClusterMetricsHelper,
  observer: ConsumerGroupOffsetObserver,
  delayMillis: Int,
  enabled: Boolean,
  excludeConsumerGroups: Seq[FlexibleName] = Seq.empty
)(implicit ec: ExecutionContext) extends KafkaClusterConsumerGroupOffsetsCollector
    with DefaultInstrumented
    with LazyLogging
    with MdcUtils
{
  import KafkaClusterConsumerGroupOffsetsPoller._

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.source.consumergroup")

  private val stopCollecting = new AtomicBoolean(false)
  private val pollFuture = Future { blocking { withMDC(helper.mdc)(pollConsumerGroupsOffsets()) } }
  pollFuture.crashIfFailed()

  private def getConsumerGroupsOffsets: Either[String, GetConsumerGroupOffsetsResult] = {
    val startNanoTime = System.nanoTime()
    for {
      consumerGroups <- if (enabled) {
        helper.adminClient.allConsumerGroups().toRightOrErrorMessage.map(_.map(_.groupId))
      } else {
        Right(Seq.empty)
      }
    } yield {
      val durationListConsumerGroup = durationSince(startNanoTime)
      val consumerGroupsWithExclusions = consumerGroups.filter(cg => !excludeConsumerGroups.exists(_.doesMatch(cg)))
      val ((errors, consumerGroupsOffsets), durationConsumerGroupsOffsets) = durationOf { helper.adminClient.consumerGroupOffsets(consumerGroupsWithExclusions) }
      errors.foreach { case (cg, WithUtcTimestamp(_, t)) =>
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"getConsumerGroupsOffsets(): $cg - ${t.getMessage}")
      }
      GetConsumerGroupOffsetsResult(durationListConsumerGroup, durationConsumerGroupsOffsets, errors, consumerGroupsOffsets)
    }
  }

  private def toConsumerGroupPartitionOffset(
    consumerGroupId: String,
    timestamp: OffsetDateTime,
    topicPartition: TopicPartition,
    offsetAndMetadata: OffsetAndMetadata
  ): (ConsumerGroupTopicPartition, ConsumerGroupOffset) =
    (
      ConsumerGroupTopicPartition(consumerGroupId, topicPartition),
      ConsumerGroupOffset(offsetAndMetadata.offset, offsetAndMetadata.metadata, timestamp)
    )

  private def toConsumerGroupPartitionOffsets(
    consumerGroupId: String,
    timestamp: OffsetDateTime,
    topicPartitionOffsets: Map[TopicPartition, OffsetAndMetadata]
  ): Map[ConsumerGroupTopicPartition, ConsumerGroupOffset] =
    topicPartitionOffsets.map {
      case (tp, om) => toConsumerGroupPartitionOffset(consumerGroupId, timestamp, tp, om)
    }

  private def toConsumerGroupPartitionOffsets(
    consumerGroupOffsets: Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]]
  ): Map[ConsumerGroupTopicPartition, ConsumerGroupOffset] =
    consumerGroupOffsets.flatMap { case (cg, WithUtcTimestamp(ts, topicPartitionOffsets)) => toConsumerGroupPartitionOffsets(cg, ts, topicPartitionOffsets) }

  private def toConsumerGroupPartition(
    consumerGroupId: String,
    topicPartition: TopicPartition
  ): ConsumerGroupTopicPartition = ConsumerGroupTopicPartition(consumerGroupId, topicPartition)

  private def toConsumerGroupPartitions(
    consumerGroupId: String,
    topicPartitionOffsets: Map[TopicPartition, OffsetAndMetadata]
  ): Iterable[ConsumerGroupTopicPartition] =
    topicPartitionOffsets.map { case (tp, om) => toConsumerGroupPartition(consumerGroupId, tp) }

  private def toConsumerGroupPartitions(
    consumerGroupId: String,
    topicPartitions: Iterable[TopicPartition]
  ): Iterable[ConsumerGroupTopicPartition] =
    topicPartitions.map(toConsumerGroupPartition(consumerGroupId, _))

  private def toConsumerGroupPartitions(
    consumerGroupOffsets: Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]]
  ): Iterable[ConsumerGroupTopicPartition] =
    consumerGroupOffsets.flatMap { case (cg, WithUtcTimestamp(_, topicPartitionOffsets)) => toConsumerGroupPartitions(cg, topicPartitionOffsets) }

  private def diffTopicPartitionOffsets(
    currentOffsets: Map[TopicPartition, OffsetAndMetadata],
    newOffsets: Map[TopicPartition, OffsetAndMetadata]
  ): (Map[TopicPartition, OffsetAndMetadata], Iterable[TopicPartition]) = {
    val topicPartitionsMissingFromNew = currentOffsets.collect { case (tp, om) if !newOffsets.contains(tp) => tp }

    val topicPartitionOffsetsNewOrUpdated = newOffsets.flatMap {
      case (tp, om) => currentOffsets.get(tp) match {
        case Some(currentOm) =>
          // this topic-partition exists in the current, return it only if it's different
          if (om != currentOm) Some((tp, om)) else None
        case None =>
          // this topic-partition is new
          Some((tp, om))
      }
    }

    (topicPartitionOffsetsNewOrUpdated, topicPartitionsMissingFromNew)
  }

  private def calculateDifference(
    currentConsumerGroupOffsets: Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]],
    newConsumerGroupOffsetsSuccessful: Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]],
    consumerGroupsWithErrors: Iterable[String]
  ): (Map[ConsumerGroupTopicPartition, ConsumerGroupOffset], Iterable[ConsumerGroupTopicPartition]) = {
    val currentConsumerGroupOffsetsForErrors = consumerGroupsWithErrors
      // for consumer groups which are there but there was an error while getting their offsets I use the current offsets...
      .map(cg => (cg, currentConsumerGroupOffsets.get(cg)))
      // ...if there is any
      .collect { case (cg, Some(o)) => (cg, o) }
      .toMap

    // the errors and successfully retrieved offsets' consumer groups are disjoint
    val newConsumerGroupOffsets = newConsumerGroupOffsetsSuccessful ++ currentConsumerGroupOffsetsForErrors

    // there was already a current consumer group offsets map => diff-ing the current and the new
    val consumerGroupsMissingFromNew = toConsumerGroupPartitions(
      currentConsumerGroupOffsets.collect { case (cg, o) if !newConsumerGroupOffsets.contains(cg) => (cg, o) }
    )
    val (consumerGroupOffsetsNewOrUpdated, moreConsumerGroupsMissingFromNew) = newConsumerGroupOffsets
      .toSeq
      .map {
        case (cg, WithUtcTimestamp(newTs, newTopicPartitionOffsets)) =>
          currentConsumerGroupOffsets.get(cg) match {
            case Some(WithUtcTimestamp(currentTs, currentTopicPartitionOffsets)) =>
              // there are already offsets for this consumer group
              val (topicPartitionOffsetsNewOrUpdated, topicPartitionsMissingFromNew) = diffTopicPartitionOffsets(currentTopicPartitionOffsets, newTopicPartitionOffsets)
              (
                toConsumerGroupPartitionOffsets(cg, newTs, topicPartitionOffsetsNewOrUpdated),
                toConsumerGroupPartitions(cg, topicPartitionsMissingFromNew)
              )

            case None =>
              // it's a completely new consumer group
              (
                toConsumerGroupPartitionOffsets(cg, newTs, newTopicPartitionOffsets),
                Iterable.empty
              )
          }
      }
      .foldLeft((Map.empty[ConsumerGroupTopicPartition, ConsumerGroupOffset], Iterable.empty[ConsumerGroupTopicPartition]))(
        (i1, i2) => {
          val (consumerGroupPartitionOffsets1, topicPartitionsMissingFromNew1) = i1
          val (consumerGroupPartitionOffsets2, topicPartitionsMissingFromNew2) = i2
          (consumerGroupPartitionOffsets1 ++ consumerGroupPartitionOffsets2, topicPartitionsMissingFromNew1 ++ topicPartitionsMissingFromNew2)
        }
      )

    (consumerGroupOffsetsNewOrUpdated, consumerGroupsMissingFromNew ++ moreConsumerGroupsMissingFromNew)
  }

  private def pollConsumerGroupsOffsets(): Unit = {
    var currentConsumerGroupOffsetsOrNone: Option[Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]]] = None
    while (!stopCollecting.get()) {
      val offsetsResult = getConsumerGroupsOffsets
      // if it completely failed (even getting the list of consumer groups) there is not much we can do, log and try again later
      offsetsResult.swap.foreach { error =>
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"getConsumerGroupsOffsets(): failed - $error")
      }
      offsetsResult.foreach { r =>
        val (_, diffAndNotifyDuration) = durationOf {
          currentConsumerGroupOffsetsOrNone match {
            case Some(currentConsumerGroupOffsets) =>
              // there is a current set of consumer group offsets so we can diff the new one to that
              val (consumerGroupOffsetsNewOrUpdated, consumerGroupsMissingFromNew) = calculateDifference(currentConsumerGroupOffsets, r.offsets, r.errors.keys)
              observer.updateConsumerGroupOffsets(helper.kafkaClusterId, consumerGroupOffsetsNewOrUpdated, consumerGroupsMissingFromNew)

            case None =>
              // there is no current so can't diff just notify the observer with the snapshot
              observer.updateAllConsumerGroupOffsets(helper.kafkaClusterId, toConsumerGroupPartitionOffsets(r.offsets))
          }

          // the current becomes the new
          currentConsumerGroupOffsetsOrNone = Some(r.offsets)
        }

        metrics.timer(s"${helper.kafkaClusterId}:listconsumergroups").update(r.listConsumerGroupsDuration)
        metrics.timer(s"${helper.kafkaClusterId}:getconsumergroupoffsets").update(r.getConsumerGroupOffsetsDuration)
        metrics.timer(s"${helper.kafkaClusterId}:notifyobservers").update(diffAndNotifyDuration)

        logger.info(s"getConsumerGroupsOffsets(${if (enabled) "enabled" else "disabled"}): ${r.errors.size} errors - ${r.offsets.size} consumer groups' offsets (${r.offsets.values.map(_.value.size).sum}) in ${r.listConsumerGroupsDuration.toMillis} + ${r.getConsumerGroupOffsetsDuration.toMillis} ms, notified observers in ${diffAndNotifyDuration.toMillis} ms")
      }

      Thread.sleep(delayMillis)
    }
  }

  def stop(): Unit = {
    stopCollecting.set(true)
    Await.result(pollFuture, Duration.Inf)
  }
}
