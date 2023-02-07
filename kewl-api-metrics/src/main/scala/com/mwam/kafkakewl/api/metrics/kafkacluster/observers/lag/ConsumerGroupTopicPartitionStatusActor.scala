/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import akka.actor.{Actor, ActorRef, Props, Terminated}
import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.ConsumeAfterConsumerGroupOffsetActor.{ConsumeAfterConsumerGroupOffset, ConsumeAfterConsumerGroupOffsetResult}
import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupMetricsObserver, ConsumerGroupTopicPartition}
import com.mwam.kafkakewl.common.ActorPreRestartLog
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupMetrics, ConsumerGroupOffset, ConsumerGroupStatus, PartitionLowHighOffset}
import com.mwam.kafkakewl.domain.{FlexibleName, Mdc}
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

import java.time.{Clock, OffsetDateTime}
import java.util.UUID
import scala.collection.mutable

object ConsumerGroupTopicPartitionStatusActor {
  def props(
    kafkaClusterId: KafkaClusterEntityId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition,
    initialStatusEvaluatorConfig: ConsumerGroupStatusEvaluatorConfig,
    observer: ConsumerGroupMetricsObserver,
    consumeAfterConsumerGroupOffsetEnabled: Boolean,
    consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long,
    consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName],
    consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName],
    nowFunc: () => OffsetDateTime = () => OffsetDateTime.now(Clock.systemUTC)
  ) = Props(
    new ConsumerGroupTopicPartitionStatusActor(
      kafkaClusterId,
      consumerGroupTopicPartition,
      initialStatusEvaluatorConfig,
      observer,
      consumeAfterConsumerGroupOffsetEnabled,
      consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix,
      consumeAfterConsumerGroupOffsetExcludeTopics,
      consumeAfterConsumerGroupOffsetExcludeConsumerGroups,
      nowFunc
    )
  )

  def name(
    kafkaClusterId: KafkaClusterEntityId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition
  ) = s"ConsumerGroupTopicPartitionStatusActor-$kafkaClusterId-${consumerGroupTopicPartition.toActorNameString}-${UUID.randomUUID().toString.replace("-", "")}"

  private[lag] final case class Offsets(
    timestampUtc: OffsetDateTime,
    consumerGroupOffset: Option[ConsumerGroupOffset],
    adjustedConsumerGroupOffset: Option[ConsumerGroupOffset], // another optional consumer group offset in case we could make an adjustment - this one is usually used to calculate lag for the status,
                                                              // the original one is used to calculate consume rate
    partitionLowHighOffset: Option[PartitionLowHighOffset]
  ) {
    lazy val adjustedConsumerGroupOffsetIfAvailable: Option[ConsumerGroupOffset] = adjustedConsumerGroupOffset.orElse(consumerGroupOffset)

    def lag(adjusted: Boolean = false): Option[Long] = for {
      partitionLowHighOffset <- partitionLowHighOffset
      consumerGroupOffset <- if (adjusted) adjustedConsumerGroupOffsetIfAvailable else consumerGroupOffset
    } yield
      // if it's negative (meaning the high offset is out-of-date) we consider it as zero
      Math.max(0L, partitionLowHighOffset.highOffset - consumerGroupOffset.offset)
  }

  private[lag] final case class SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor: ActorRef)

  // request messages for unit-testing
  private[lag] final case object GetOffsetsWindow
  private[lag] final case object GetConsumeAfterConsumerGroupOffsetActor
  private[lag] final case object GetPendingConsumeAfterConsumerGroupOffset
  private[lag] final case object GetConsumeAfterConsumerGroupOffsetResult
}

class ConsumerGroupTopicPartitionStatusActor(
  kafkaClusterId: KafkaClusterEntityId,
  consumerGroupTopicPartition: ConsumerGroupTopicPartition,
  initialStatusEvaluatorConfig: ConsumerGroupStatusEvaluatorConfig,
  observer: ConsumerGroupMetricsObserver,
  consumeAfterConsumerGroupOffsetEnabled: Boolean,
  consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long,
  consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName],
  consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName],
  nowFunc: () => OffsetDateTime = () => OffsetDateTime.now(Clock.systemUTC)
) extends Actor
  with ActorPreRestartLog
  with ConsumerGroupStatusEvaluatorUtils
  with DefaultInstrumented
  with LazyLogging
  with MdcUtils
{
  import ConsumerGroupTopicPartitionStatusActor._

  override val actorMdc: Map[String, String] = Mdc.fromKafkaClusterId(kafkaClusterId)

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.source.consumergroup")

  private var statusEvaluatorConfig = initialStatusEvaluatorConfig

  withMDC(actorMdc) {
    logger.info(s"$consumerGroupTopicPartition: slidingWindowSize = ${statusEvaluatorConfig.slidingWindowSize}, consumeAfterConsumerGroupOffset = {enabled = $consumeAfterConsumerGroupOffsetEnabled, maxTopicPartitionLagToFix = $consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix, excludeTopics = $consumeAfterConsumerGroupOffsetExcludeTopics, excludeConsumerGroups = $consumeAfterConsumerGroupOffsetExcludeConsumerGroups}")
  }

  // storing the last partition/consumer-group offsets
  private var lastPartitionLowHighOffsetOrNone: Option[PartitionLowHighOffset] = None
  private var lastConsumerGroupOffsetOrNone: Option[ConsumerGroupOffset] = None

  // the sliding window that keeps the last data-points
  private val offsetsWindow = mutable.Queue.empty[Offsets]
  private var lastMetrics: Option[ConsumerGroupMetrics] = None

  // the actor that can consume topics to confirm whether the lag is real or not
  private var consumeAfterConsumerGroupOffsetActorOrNone: Option[ActorRef] = None

  // the current pending consume request
  private var pendingConsumeAfterConsumerGroupOffsetOrNone: Option[ConsumeAfterConsumerGroupOffset] = None
  // the last result of the topic partition consume
  private var consumeAfterConsumerGroupOffsetResultOrNone: Option[ConsumeAfterConsumerGroupOffsetResult] = None

  private def consumerGroupMetrics: ConsumerGroupMetrics = {
    ConsumerGroupMetrics(
      offsetsWindow.consumerGroupStatus(statusEvaluatorConfig),
      lastPartitionLowHighOffsetOrNone.map(_.partitionHighOffset),
      lastConsumerGroupOffsetOrNone,
      lastOffsets.lag(adjusted = false),
      offsetsWindow.consumptionRate
    )
  }

  private def consumeAfterConsumerGroupOffset(consumerGroupOffset: Long, endOffset: Long): Unit = {
    pendingConsumeAfterConsumerGroupOffsetOrNone match {
      case Some(_) =>
        // There is a pending request already, maybe with a different (earlier) consumer group offset.
        // Still, let's just not do anything, be gentle on kafka. Maybe the pending one will cover this consumer group offset too. If not, we'll request it again anyway later...
      case None =>
        val consumerGroupExcluded = consumeAfterConsumerGroupOffsetExcludeConsumerGroups.exists(_.doesMatch(consumerGroupTopicPartition.consumerGroupId))
        val topicExcluded = consumeAfterConsumerGroupOffsetExcludeTopics.exists(_.doesMatch(consumerGroupTopicPartition.topicPartition.topic))
        val lag = Math.max(0L, endOffset - consumerGroupOffset)
        val lagMoreThanMaximum = lag > consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix
        if (consumeAfterConsumerGroupOffsetEnabled && !lagMoreThanMaximum && !topicExcluded && !consumerGroupExcluded) {
          consumeAfterConsumerGroupOffsetActorOrNone.foreach { consumeAfterConsumerGroupOffsetActor =>
            val consumeAfterConsumerGroupOffset = ConsumeAfterConsumerGroupOffsetActor.ConsumeAfterConsumerGroupOffset(
              consumerGroupTopicPartition.topicPartition,
              consumerGroupTopicPartition.consumerGroupId,
              consumerGroupOffset,
              endOffset
            )
            consumeAfterConsumerGroupOffsetActor ! consumeAfterConsumerGroupOffset

            pendingConsumeAfterConsumerGroupOffsetOrNone = consumeAfterConsumerGroupOffset.some
            metrics.counter(s"$kafkaClusterId:pendingconsumeafterconsumergroupoffset").inc()
            metrics.meter(s"$kafkaClusterId:requestedconsumeafterconsumergroupoffset").mark()
          }
        }
    }
  }

  private def adjustConsumerGroupOffsetIfPossible(
    consumerGroupOffset: Long,
    partitionLowHighOffset: Option[PartitionLowHighOffset],
    consumeAfterConsumerGroupOffsetResultOrNone: Option[ConsumeAfterConsumerGroupOffsetResult]
  ): Long = {
    // move the consumer group offset forward to the lowOffset (if there is one)
    val consumerGroupOffsetAfterLowOffset = partitionLowHighOffset
      .map { plho => math.max(plho.lowOffset, consumerGroupOffset) }
      .getOrElse(consumerGroupOffset)

    consumeAfterConsumerGroupOffsetResultOrNone match {
      case Some(ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(_, noMessagesFrom, noMessagesTo, _)) =>
        if (consumerGroupOffsetAfterLowOffset >= noMessagesFrom && consumerGroupOffsetAfterLowOffset <= noMessagesTo) {
          // we know that there aren't any messages after the consumer group offset until noMessagesTo, so we can pretend we committed noMessagesTo (it may or may not be a message, but it doesn't matter)
          val correction = noMessagesTo - consumerGroupOffset
          if (correction > 0) {
            metrics.meter(s"$kafkaClusterId:adjustedconsumergroupoffset").mark()
            metrics.counter(s"$kafkaClusterId:consumergroupoffsetadjustment").inc(correction)
          }
          noMessagesTo
        } else {
          // We don't really know what's after the consumer group offset (it either got before noMessagesFrom which is possible only if the consumer committed an earlier offset OR it's after noMessagesTo)
          // In this case we really should query the next message offset after consumer group offset again, but I don't do it here, because it may be too wasteful if the overall status is OK
          metrics.meter(s"$kafkaClusterId:couldnotadjustconsumergroupoffset").mark()
          consumerGroupOffsetAfterLowOffset
        }

      case _ =>
        // we can't fix it we don't know what's after
        consumerGroupOffsetAfterLowOffset
    }
  }

  private def consumeAfterConsumerGroupOffsetIfNeeded(
    consumerGroupOffset: Long,
    endOffset: Long,
    consumeAfterConsumerGroupOffsetResultOrNone: Option[ConsumeAfterConsumerGroupOffsetResult]
  ): Unit = {
    consumeAfterConsumerGroupOffsetResultOrNone match {
      case Some(ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(_, noMessagesFrom, noMessagesTo, false)) =>
        // we have no message at noMessagesTo (end of topic-partition)
        if (consumerGroupOffset >= noMessagesFrom && consumerGroupOffset <= noMessagesTo) {
          // no need to consume yet (we could fix the consumerGroupOffset, we're good for now)
        } else {
          // yep, need to consume, the current NoMessagesBetweenOffsets() is obsolete
          consumeAfterConsumerGroupOffset(consumerGroupOffset, endOffset)
        }

      case Some(ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(_, _, noMessagesTo, true)) =>
        // we have a message at noMessagesTo
        if (consumerGroupOffset <= noMessagesTo) {
          // We don't bother consuming if the consumer group offset is anywhere below noMessagesTo, because we know that there is real lag, although maybe not fully accurate lag
          // Technically, we could trigger the consume if the consumer group offset is below noMessagesFrom, but it's not a big deal if we don't do that, the lag won't be accurate but accurate enough
          // (since we won't be able to fix it)
        } else {
          // we don't know what's after the consumer group offset, need to consume
          consumeAfterConsumerGroupOffset(consumerGroupOffset, endOffset)
        }

      case _ =>
        // don't know anything, need to consume
        consumeAfterConsumerGroupOffset(consumerGroupOffset, endOffset)
    }
  }

  private def appendToOffsetsWindow(
    newOffsets: Offsets,
    consumeAfterConsumerGroupOffsetResultOrNone: Option[ConsumeAfterConsumerGroupOffsetResult]
  ): Unit = {
    // attempting to "fix" the consumer group offset: we can fast-forward to skip transaction markers and rolled-back messages
    // so that the lag is reduced and more accurate (possibly even goes down to zero)
    val fixedNewOffsets = newOffsets.copy(
      // adjust if it's not adjusted already (shouldn't be)
      adjustedConsumerGroupOffset = newOffsets.adjustedConsumerGroupOffset.orElse {
        newOffsets.consumerGroupOffset.flatMap { cgo =>
          val adjustedOffset = adjustConsumerGroupOffsetIfPossible(cgo.offset, newOffsets.partitionLowHighOffset, consumeAfterConsumerGroupOffsetResultOrNone)
          if (cgo.offset != adjustedOffset) {
            cgo.copy(offset = adjustedOffset).some
          } else {
            // no adjustment, they are the same
            none
          }
        }
      }
    )

    // appending to the window with the current time-stamp
    offsetsWindow.enqueue(fixedNewOffsets)

    // removing the too old offsets if there are any
    while (offsetsWindow.headOption.exists(offsets => fixedNewOffsets.timestampUtc.minusNanos(statusEvaluatorConfig.slidingWindowSize.toNanos).isAfter(offsets.timestampUtc))) {
      offsetsWindow.dequeue()
    }

    // calculating the new metrics
    val metrics = consumerGroupMetrics
    lastMetrics = Some(metrics)

    // we do this before updating the observers so that topic consumption starts as early as possible in the background
    if (metrics.status == ConsumerGroupStatus.Stopped || metrics.status == ConsumerGroupStatus.MaybeStopped || metrics.status == ConsumerGroupStatus.Error) {
      // If the status is some kind of error, we try to consume the topic from the consumer group offset to see
      // if there are any messages after (i.e. the lag is valid) OR only uncommitted messages/transactional markers (i.e. the lag is fake).
      // We try to consume as little and as rarely as possible, see the implementation of consumeAfterConsumerGroupOffsetIfNeeded().
      (fixedNewOffsets.consumerGroupOffset, fixedNewOffsets.partitionLowHighOffset) match {
        case (Some(ConsumerGroupOffset(consumerGroupOffset, _, _)), Some(PartitionLowHighOffset(_, highOffset, _))) =>
          consumeAfterConsumerGroupOffsetIfNeeded(consumerGroupOffset, highOffset, consumeAfterConsumerGroupOffsetResultOrNone)
        case _ =>
          // not trying to consume the topic, since we don't have both consumer group offset and end offset
      }
    } else {
      // if the status is OK, we don't bother consuming the topic
    }

    observer.updateConsumerGroupMetrics(kafkaClusterId, consumerGroupTopicPartition, metrics)
  }

  private def lastOffsets: Offsets = Offsets(
    nowFunc(),
    consumerGroupOffset = lastConsumerGroupOffsetOrNone,
    adjustedConsumerGroupOffset = None,
    lastPartitionLowHighOffsetOrNone
  )

  override def postStop(): Unit = {
    super.postStop()
    if (pendingConsumeAfterConsumerGroupOffsetOrNone.nonEmpty) {
      // so that it doesn't remain stuck
      metrics.counter(s"$kafkaClusterId:pendingconsumeafterconsumergroupoffset").dec()
    }
  }

  override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    withMDC(actorMdc) {
      super.aroundReceive(receive, msg)
    }
  }

  override def receive: Receive = {
    case partitionLowHighOffset: PartitionLowHighOffset =>
      val topicWasRecreated = lastPartitionLowHighOffsetOrNone
        .exists { lastPartitionLowHighOffset =>
          partitionLowHighOffset.lowOffset < lastPartitionLowHighOffset.lowOffset || partitionLowHighOffset.highOffset < lastPartitionLowHighOffset.highOffset
        }
      if (topicWasRecreated) {
        logger.info(s"$consumerGroupTopicPartition: topic was re-created - last = ${lastPartitionLowHighOffsetOrNone}, current = $partitionLowHighOffset")
        // if the topic was re-created we get rid of any pending consume and current consume result, because most likely that will be invalid
        // other things, like the lag windows, etc... are fine to leave as they are
        pendingConsumeAfterConsumerGroupOffsetOrNone = None
        consumeAfterConsumerGroupOffsetResultOrNone = None
      }

      lastPartitionLowHighOffsetOrNone =
        lastPartitionLowHighOffsetOrNone
          .map { lastPartitionLowHighOffset =>
            // keeping the new offsets and time-stamp only if it's really a new one (hence the same low/high offset's timestamp won't overwrite the previous)
            if (partitionLowHighOffset.lowOffset != lastPartitionLowHighOffset.lowOffset || partitionLowHighOffset.highOffset != lastPartitionLowHighOffset.highOffset) partitionLowHighOffset
            else lastPartitionLowHighOffset
          }
          .getOrElse(partitionLowHighOffset)
          .some

      appendToOffsetsWindow(lastOffsets, consumeAfterConsumerGroupOffsetResultOrNone)
      if (sender != context.system.deadLetters) lastMetrics.foreach(sender ! _)

    case consumerGroupOffset: ConsumerGroupOffset =>
      lastConsumerGroupOffsetOrNone = Some(consumerGroupOffset)
      appendToOffsetsWindow(lastOffsets, consumeAfterConsumerGroupOffsetResultOrNone)
      if (sender != context.system.deadLetters) lastMetrics.foreach(sender ! _)

    case consumerGroupMetricsConfig: ConsumerGroupMetricsConfig =>
      val newSlidingWindowSize = consumerGroupMetricsConfig.slidingWindowSize.getOrElse(initialStatusEvaluatorConfig.slidingWindowSize)
      if (statusEvaluatorConfig.slidingWindowSize != newSlidingWindowSize) {
        logger.info(s"$consumerGroupTopicPartition: slidingWindowSize changed ${statusEvaluatorConfig.slidingWindowSize} -> $newSlidingWindowSize")
        statusEvaluatorConfig = statusEvaluatorConfig.copy(slidingWindowSize = newSlidingWindowSize)
        // not doing anything more here, the next time we receive an offset it'll be used
      }

    case SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor) =>
      consumeAfterConsumerGroupOffsetActorOrNone = consumeAfterConsumerGroupOffsetActor.some
      context.watch(consumeAfterConsumerGroupOffsetActor)

    case Terminated(actor) if (consumeAfterConsumerGroupOffsetActorOrNone.contains(actor)) =>
      consumeAfterConsumerGroupOffsetActorOrNone = None

    case consumeAfterConsumerGroupOffsetResult: ConsumeAfterConsumerGroupOffsetResult =>
      pendingConsumeAfterConsumerGroupOffsetOrNone
        // do nothing if this response doesn't belong to the current pending request
        .filter(_ == consumeAfterConsumerGroupOffsetResult.request) match {
          case Some (_) =>
            consumeAfterConsumerGroupOffsetResultOrNone = consumeAfterConsumerGroupOffsetResult.some
            pendingConsumeAfterConsumerGroupOffsetOrNone = None
            metrics.counter(s"$kafkaClusterId:pendingconsumeafterconsumergroupoffset").dec()
            appendToOffsetsWindow(lastOffsets, consumeAfterConsumerGroupOffsetResultOrNone)
            if (sender != context.system.deadLetters) lastMetrics.foreach(sender ! _)
          case None =>
            logger.info(s"ignoring $consumeAfterConsumerGroupOffsetResult, most likely because the topic was re-created")
      }

    case GetOffsetsWindow =>
      // for unit-tests only
      sender ! Seq(offsetsWindow: _*)

    case GetConsumeAfterConsumerGroupOffsetActor =>
      // for unit-tests only
      sender ! consumeAfterConsumerGroupOffsetActorOrNone

    case GetPendingConsumeAfterConsumerGroupOffset =>
      // for unit-tests only
      sender ! pendingConsumeAfterConsumerGroupOffsetOrNone

    case GetConsumeAfterConsumerGroupOffsetResult =>
      // for unit-tests only
      sender ! consumeAfterConsumerGroupOffsetResultOrNone
  }
}
