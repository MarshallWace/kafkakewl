/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import java.time.OffsetDateTime
import scala.concurrent.duration._
import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.ConsumerGroupTopicPartitionStatusActor.Offsets
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupOffset, ConsumerGroupStatus, PartitionHighOffset, PartitionLowHighOffset}
import org.scalatest.{Matchers, WordSpec}

class ConsumerGroupStatusEvaluatorUtilsSpec extends WordSpec
  with Matchers
  with ConsumerGroupStatusEvaluatorUtils {

  private def ts(isoString: String): OffsetDateTime = OffsetDateTime.parse(isoString + "+00:00")
  private def offsets(timestampUtcIso: String) =
    Offsets(ts(timestampUtcIso), None, None, None)
  private def offsets(timestampUtcIso: String, consumerGroupOffset: ConsumerGroupOffset, partitionHighOffset: PartitionHighOffset) =
    Offsets(ts(timestampUtcIso), consumerGroupOffset.some, None, partitionHighOffset.withZeroLowOffset.some)
  private def offsets(timestampUtcIso: String, consumerGroupOffset: ConsumerGroupOffset, adjustedConsumerGroupOffset: ConsumerGroupOffset, partitionHighOffset: PartitionHighOffset) =
    Offsets(ts(timestampUtcIso), consumerGroupOffset.some, adjustedConsumerGroupOffset.some, partitionHighOffset.withZeroLowOffset.some)
  private def offsets(timestampUtcIso: String, consumerGroupOffset: ConsumerGroupOffset, partitionLowHighOffset: PartitionLowHighOffset) =
    Offsets(ts(timestampUtcIso), consumerGroupOffset.some, None, partitionLowHighOffset.some)
  private def offsets(timestampUtcIso: String, consumerGroupOffset: ConsumerGroupOffset) =
    Offsets(ts(timestampUtcIso), consumerGroupOffset.some, None, None)
  private def offsets(timestampUtcIso: String, partitionHighOffset: PartitionHighOffset) =
    Offsets(ts(timestampUtcIso), None, None, partitionHighOffset.withZeroLowOffset.some)
  private def offsets(timestampUtcIso: String, partitionLowHighOffset: PartitionLowHighOffset) =
    Offsets(ts(timestampUtcIso), None, None, partitionLowHighOffset.some)

  private implicit val defaultSlidingWindowSize: Duration = 10.minutes

  implicit class OffsetsWindowSpecExtensions(offsetsWindow: Seq[Offsets]) {
    def status(implicit consumerGroupOffsetMode: ConsumerGroupOffsetMode, maxWindowSize: Duration): ConsumerGroupStatus =
      offsetsWindow.consumerGroupStatus(ConsumerGroupStatusEvaluatorConfig(consumerGroupOffsetMode, maxWindowSize, 0.5))

    def statusWithActualCommit(implicit maxWindowSize: Duration): ConsumerGroupStatus =
      offsetsWindow.consumerGroupStatus(ConsumerGroupStatusEvaluatorConfig(ConsumerGroupOffsetMode.ActualCommit, maxWindowSize, 0.5))

    def statusWithSampled(implicit maxWindowSize: Duration): ConsumerGroupStatus =
      offsetsWindow.consumerGroupStatus(ConsumerGroupStatusEvaluatorConfig(ConsumerGroupOffsetMode.Sampled, maxWindowSize, 0.5))
  }

  private val modes = Iterable(ConsumerGroupOffsetMode.Sampled, ConsumerGroupOffsetMode.ActualCommit)

  "hasZeroLag" when {
    "there are no offsets in the window" should {
      "be false" in {
        Seq.empty[Offsets].hasZeroLag shouldBe false
      }
    }
    "there is a single offset with no actual offsets in the window" should {
      "be false" in {
        Seq(offsets("2019-03-18T10:10:05")).hasZeroLag shouldBe false
      }
    }
    "there is a single offset with no consumer offset in the window" should {
      "be false" in {
        Seq(offsets("2019-03-18T10:10:05", PartitionHighOffset(150L, ts("2019-03-18T10:10:05")))).hasZeroLag shouldBe false
      }
    }
    "there is a single offset with no partition offset in the window" should {
      "be false" in {
        Seq(offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")))).hasZeroLag shouldBe false
      }
    }
    "there is a single offset with non-zero lag in the window" should {
      "be false" in {
        Seq(offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05")))).hasZeroLag shouldBe false
      }
    }
    "there are a few non-zero offsets in the window" should {
      "be false" in {
        Seq(
          offsets("2019-03-18T10:10:05"),
          offsets("2019-03-18T10:10:05", PartitionHighOffset(150L, ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05")))
        ).hasZeroLag shouldBe false
      }
    }
    "there is a single offset with 0 lag in the window" should {
      "be true" in {
        Seq(offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(150L, ts("2019-03-18T10:10:05")))).hasZeroLag shouldBe true
      }
    }
    "there is a single offset with 0 lag where the consumer offset is ahead in the window" should {
      "be true" in {
        Seq(offsets("2019-03-18T10:10:05", ConsumerGroupOffset(152L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(150L, ts("2019-03-18T10:10:05")))).hasZeroLag shouldBe true
      }
    }
    "there are a few non-0-lag offsets with a 0 lag offset in the window" should {
      "be false" in {
        Seq(
          offsets("2019-03-18T10:10:05"),
          offsets("2019-03-18T10:10:05", PartitionHighOffset(150L, ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(152L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05")))
        ).hasZeroLag shouldBe true
      }
    }
    "there are a few non-0-lag offsets with a 0 lag offset where the consumer offset is ahead in the window" should {
      "be false" in {
        Seq(
          offsets("2019-03-18T10:10:05"),
          offsets("2019-03-18T10:10:05", PartitionHighOffset(150L, ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05"))),
          offsets("2019-03-18T10:10:05", ConsumerGroupOffset(154L, "", ts("2019-03-18T10:10:05")), PartitionHighOffset(152L, ts("2019-03-18T10:10:05")))
        ).hasZeroLag shouldBe true
      }
    }
  }

  "consumerGroupStatus from both source modes" when {
    "there are no offsets in the window" should {
      "be Unknown" in modes.map { implicit mode =>
        Seq.empty[Offsets].status shouldBe ConsumerGroupStatus.Unknown
      }
    }

    "there is a single 0 lag in the window" should {
      "be Ok" in modes.map { implicit mode =>
        Seq(
          offsets("2019-03-18T10:10:10", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:10:00")), PartitionHighOffset(123L, ts("2019-03-18T10:10:05")))
        ).status shouldBe ConsumerGroupStatus.Ok
      }
    }

    "there are a few offsets in the window but it's filled less than 50%" should {
      "be Unknown" in modes.map { implicit mode =>
        Seq(
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(152L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(153L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).status shouldBe ConsumerGroupStatus.Unknown
      }
    }

    "there are enough offsets and a single 0 lag in the window" should {
      "be Ok" in modes.map { implicit mode =>
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(150L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(150L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).status shouldBe ConsumerGroupStatus.Ok
      }
    }

    "there are many offsets and a single 0 lag where the consumer offset is ahead in the window" should {
      "be Ok" in modes.map { implicit mode =>
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(151L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(150L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(151L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(151L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).status shouldBe ConsumerGroupStatus.Ok
      }
    }
  }

  "consumerGroupStatus from Sampled source mode" when {
    "there are enough offsets and but the consumer offsets are all the same" should {
      "be MaybeStopped" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(150L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).statusWithSampled shouldBe ConsumerGroupStatus.MaybeStopped
      }
    }

    "there are enough offsets, some are different but the lag stays the same" should {
      "be Warning" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(151L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(152L, ts("2019-03-18T10:15:00")))
        ).statusWithSampled shouldBe ConsumerGroupStatus.Warning
      }
    }

    "there are enough offsets, some are different but the lag increases" should {
      "be Warning" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(153L, ts("2019-03-18T10:15:00")))
        ).statusWithSampled shouldBe ConsumerGroupStatus.Warning
      }
    }

    "there are enough offsets, some are different and there is lag decrease sometimes" should {
      "be Ok" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:12:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:13:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:14:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(144L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(153L, ts("2019-03-18T10:15:00")))
        ).statusWithSampled shouldBe ConsumerGroupStatus.Ok
      }
    }
  }

  "consumerGroupStatus from ActualCommit source mode" when {
    "there are enough offsets and but the consumer offsets are all the same and the last commit timestamp out of the window" should {
      "be Stopped" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).statusWithActualCommit shouldBe ConsumerGroupStatus.Stopped
      }
    }

    "there are enough offsets and but the consumer offsets are all the same and the last commit timestamp is still in the window" should {
      "be Error" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:03:00")), PartitionHighOffset(150L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:04:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(123L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).statusWithActualCommit shouldBe ConsumerGroupStatus.Error
      }
    }

    "there are enough increasing consumer offsets but the lag stays the same" should {
      "be Warning" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:03:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:04:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(152L, ts("2019-03-18T10:15:00")))
        ).statusWithActualCommit shouldBe ConsumerGroupStatus.Warning
      }
    }

    "there are enough increasing consumer offsets but the lag increases" should {
      "be Warning" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:03:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:04:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).statusWithActualCommit shouldBe ConsumerGroupStatus.Warning
      }
    }

    "there are enough increasing consumer offsets and there is lag decrease sometimes" should {
      "be Ok" in {
        Seq(
          offsets("2019-03-18T10:10:00"),
          offsets("2019-03-18T10:11:00", PartitionHighOffset(150L, ts("2019-03-18T10:11:00"))),
          offsets("2019-03-18T10:12:00", ConsumerGroupOffset(140L, "", ts("2019-03-18T10:02:00")), PartitionHighOffset(150L, ts("2019-03-18T10:12:00"))),
          offsets("2019-03-18T10:13:00", ConsumerGroupOffset(141L, "", ts("2019-03-18T10:03:00")), PartitionHighOffset(151L, ts("2019-03-18T10:13:00"))),
          offsets("2019-03-18T10:14:00", ConsumerGroupOffset(142L, "", ts("2019-03-18T10:04:00")), PartitionHighOffset(152L, ts("2019-03-18T10:14:00"))),
          offsets("2019-03-18T10:15:00", ConsumerGroupOffset(145L, "", ts("2019-03-18T10:15:00")), PartitionHighOffset(154L, ts("2019-03-18T10:15:00")))
        ).statusWithActualCommit shouldBe ConsumerGroupStatus.Ok
      }
    }
  }
}
