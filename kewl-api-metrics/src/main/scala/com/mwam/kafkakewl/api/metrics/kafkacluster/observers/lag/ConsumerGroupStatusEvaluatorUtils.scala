/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.ConsumerGroupTopicPartitionStatusActor.Offsets
import com.mwam.kafkakewl.domain.metrics.ConsumerGroupStatus

import scala.concurrent.duration._

/**
  * A consumer offset can have slightly different meanings, depending on how the consumer offset collector got it.
  */
sealed trait ConsumerGroupOffsetMode
object ConsumerGroupOffsetMode {
  def apply(modeString: String): ConsumerGroupOffsetMode = modeString.toLowerCase match {
    case "actualcommit" | "consumer" => ActualCommit
    case "poller" | "polling" | "sampled" => Sampled
  }

  /**
    * The offset is a result of an actual commit operation, and its time-stamp is the commit time-stamp.
    *
    * It's applicable when the consumer offset collector consumes the `__consumer_offsets topic`.
    */
  final case object ActualCommit extends ConsumerGroupOffsetMode

  /**
    * The offset is a result of reading the currently committed offsets for a consumer group, the time-stamp is really just the sampling time-stamp,
    * we don't know when the commit happened.
    *
    * Also, because the poller notifies the offset window about different offsets only, we don't receive repeatedly committed offsets.
    *
    * It's applicable when the consumer offset collector polls the groups' offsets.
    */
  final case object Sampled extends ConsumerGroupOffsetMode
}

/**
  * Contains the configuration for the consumer group status evaluation algorithm.
  *
  * @param consumerGroupOffsetMode the consumer group offset source's mode (ActualCommit or Sampled)
  * @param slidingWindowSize the maximum size of the sliding window
  * @param expectedWindowFillFactor the minimum fill-factor of the sliding window that we expect to be able to tell any status other than Unknown.
  */
final case class ConsumerGroupStatusEvaluatorConfig(
  consumerGroupOffsetMode: ConsumerGroupOffsetMode,
  slidingWindowSize: Duration,
  expectedWindowFillFactor: Double
)

private[lag] object ConsumerGroupStatusEvaluatorUtils {
  sealed trait ConsumerOffsetsDistribution
  final case object ConsumerOffsetsAreSame extends ConsumerOffsetsDistribution
  final case object ConsumerOffsetsAreNotTheSame extends ConsumerOffsetsDistribution

  sealed trait LagTrend
  final case object MonotonicallyIncreasing extends LagTrend
  final case object HasDecreasingPart extends LagTrend

  sealed trait LastConsumerOffsetTimestampAge
  final case object LastConsumerOffsetOutOfWindow extends LastConsumerOffsetTimestampAge
  final case object LastConsumerOffsetWithinWindow extends LastConsumerOffsetTimestampAge
}

trait ConsumerGroupStatusEvaluatorUtils {
  import ConsumerGroupStatusEvaluatorUtils._

  /**
    * Extensions for a sequence (the last window) of Offsets.
    *
    * Offsets can contain None partition or consumer group offsets only at the beginning, once it contains some value, it never goes back to None.
    */
  implicit class OffsetsWindowExtensions(offsetsWindow: Seq[Offsets]) {
    // HACK: we consider 0 and 1 as zero-lag, because the committed offset may be followed by a transaction marker, in which case it's not really a lag
    private[lag] def hasZeroLag: Boolean = offsetsWindow.exists(_.lag(adjusted = true).exists(_ <= 1L))
    //private[lag] def hasZeroLag: Boolean = offsetsWindow.exists(_.lag.exists(_ <= 0L))

    private[lag] def windowDuration: Option[Duration] =
      for {
        first <- offsetsWindow.headOption
        last <- offsetsWindow.lastOption
      } yield Duration.fromNanos(java.time.Duration.between(first.timestampUtc, last.timestampUtc).toNanos)

    private[lag] def consumerOffsetsAreSame: ConsumerOffsetsDistribution = {
      val distinctOffsets = offsetsWindow
        // here we don't use adjusted offsets, because we really want to see what the consumer committed
        .collect { case Offsets(_, Some(cgo), _, _) => cgo.offset }
        .distinct
      // if distinctOffsets == 0, we consider them to have the same offsets, as well as when distinctOffsets == 1
      if (distinctOffsets.size < 2) ConsumerOffsetsAreSame else ConsumerOffsetsAreNotTheSame
    }

    private[lag] def lastConsumerOffsetTimestampTooOld: LastConsumerOffsetTimestampAge = {
      val lastConsumerOffsetTimestampOutOfWindow = offsetsWindow.nonEmpty &&
        offsetsWindow.last.consumerGroupOffset.exists(cgo => offsetsWindow.head.timestampUtc.isAfter(cgo.lastOffsetsTimestampUtc))

      if (lastConsumerOffsetTimestampOutOfWindow) LastConsumerOffsetOutOfWindow else LastConsumerOffsetWithinWindow
    }

    private[lag] def nonConstantMonotonicallyIncreasingLag: LagTrend = {
      // we don't use adjusted offsets, because an adjustment can fake that the lag decreased briefly
      val lags = offsetsWindow.flatMap(_.lag(adjusted = false))
      // yes, it can be with fold/scan/etc... but this is simply easier to understand
      var previousLag = 0L
      var foundDecreasingLag = false
      for (lag <- lags) {
        if (lag < previousLag) {
          foundDecreasingLag = true
        }
        previousLag = lag
      }

      if (foundDecreasingLag) HasDecreasingPart else MonotonicallyIncreasing
    }

    private[lag] def consumptionRate: Option[Double] = {
      // "offsetsWindow" contains the window for lag status calculation with a configurable duration (default = 5 mins)
      // It's length is supposed to cover multiple commits to make sure it's meaningful, which makes it suitable for consumption rate calculation too
      for {
        lastOffsets <- offsetsWindow.lastOption                     // if there is no last (the window is empty), it's fine to return None
        lastConsumerGroupOffset <- lastOffsets.consumerGroupOffset  // if the last one doesn't have committed offset, none of the previous one can have, so it's fine to return None
        // search the first non-None consumerGroupOffset from the beginning that's before the last one
        // yeah, linear search, not ideal (these windows are fairly small though, but there are lots of them)
        firstOffsets <- offsetsWindow.find(_.consumerGroupOffset.exists(_.offset < lastConsumerGroupOffset.offset))
        firstConsumerGroupOffsets <- firstOffsets.consumerGroupOffset
        // the interval is the whole window duration, not just the diff between the offsets' timestamps so that the denominator is roughly the same
        intervalSeconds <- windowDuration.map(_.toMillis).filter(_ > 0).map(_.toDouble / 1000.0)
      } yield (lastConsumerGroupOffset.offset - firstConsumerGroupOffsets.offset).toDouble / intervalSeconds
    }

    def consumerGroupStatus(config: ConsumerGroupStatusEvaluatorConfig): ConsumerGroupStatus = {
      import ConsumerGroupOffsetMode._

      if (hasZeroLag) {
        // if the window has a single data-point with zero lag, we consider it as an OK consumer no matter what
        ConsumerGroupStatus.Ok
      } else if (offsetsWindow.isEmpty || windowDuration.exists(_.lt(config.slidingWindowSize * config.expectedWindowFillFactor))) {
        // if the window is empty OR it's too small
        ConsumerGroupStatus.Unknown
      } else {
        // there are "enough" data-points and all of them have positive lag

        (config.consumerGroupOffsetMode, consumerOffsetsAreSame, lastConsumerOffsetTimestampTooOld, nonConstantMonotonicallyIncreasingLag) match {
          // a polling consumer doesn't produce new offsets - it may or may not be stopped, we don't know whether it's actually not committing anything
          // OR just committing the same offsets
          case (Sampled, ConsumerOffsetsAreSame, _, _) => ConsumerGroupStatus.MaybeStopped

          // polling consumer has a proper new offset but the lag increases
          case (Sampled, ConsumerOffsetsAreNotTheSame, _, MonotonicallyIncreasing) => ConsumerGroupStatus.Warning

          // polling consumer has a proper new offset and the lag is constant has decreasing parts
          case (Sampled, ConsumerOffsetsAreNotTheSame, _, HasDecreasingPart) => ConsumerGroupStatus.Ok

          // proper consumer doesn't commit at all
          case (ActualCommit, ConsumerOffsetsAreSame, LastConsumerOffsetOutOfWindow, _) => ConsumerGroupStatus.Stopped

          // proper consumer doesn't produce new offsets but last committed is still in the window (it's not stopped, but close)
          case (ActualCommit, ConsumerOffsetsAreSame, LastConsumerOffsetWithinWindow, _) => ConsumerGroupStatus.Error

          // proper consumer has a proper new offset but the lag is increasing
          case (ActualCommit, ConsumerOffsetsAreNotTheSame, _, MonotonicallyIncreasing) => ConsumerGroupStatus.Warning

          // polling consumer has a proper new offset and the lag has decreasing parts
          case (ActualCommit, ConsumerOffsetsAreNotTheSame, _, HasDecreasingPart) => ConsumerGroupStatus.Ok
        }
      }
    }
  }
}
