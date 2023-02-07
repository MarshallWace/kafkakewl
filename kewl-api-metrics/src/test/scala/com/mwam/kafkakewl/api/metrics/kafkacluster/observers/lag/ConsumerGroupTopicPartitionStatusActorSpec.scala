/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit}
import cats.syntax.option._
import com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag.ConsumeAfterConsumerGroupOffsetActor.{ConsumeAfterConsumerGroupOffset, ConsumeAfterConsumerGroupOffsetResult, SetKafkaConnection}
import com.mwam.kafkakewl.api.metrics.kafkacluster.{ConsumerGroupMetricsObserver, ConsumerGroupTopicPartition}
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupMetrics, ConsumerGroupOffset, ConsumerGroupStatus, PartitionHighOffset, PartitionLowHighOffset}
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import org.apache.kafka.common.TopicPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import java.time.{Clock, OffsetDateTime}
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

private object ConsumerGroupTopicPartitionStatusActorSpec {
  class CurrentTime(initial: OffsetDateTime) {
    private var current = initial

    def +=(duration: Duration): CurrentTime = {
      current = current.plusNanos(duration.toNanos)
      this
    }

    def plusSeconds(seconds: Long): CurrentTime = this += Duration(seconds, TimeUnit.SECONDS)
    def plusMinutes(minutes: Long): CurrentTime = this += Duration(minutes, TimeUnit.MINUTES)

    def now: OffsetDateTime = current
    val nowFunc: () => OffsetDateTime = now _
  }

  final case object GetKafkaConnection
  final case object GetConsumeAfterConsumerGroupOffsets
  final case object GetLastConsumeAfterConsumerGroupOffset

  class MockConsumeAfterConsumerGroupOffsetActor extends Actor {

    private var kafkaConnection: Option[KafkaConnection] = None
    private var consumeAfterConsumerGroupOffsets = Seq.empty[ConsumeAfterConsumerGroupOffset]

    override def receive: Receive = {
      case SetKafkaConnection(kafkaConnection) =>
        this.kafkaConnection = kafkaConnection.some

      case consumeAfterConsumerGroupOffset: ConsumeAfterConsumerGroupOffset =>
        this.consumeAfterConsumerGroupOffsets = this.consumeAfterConsumerGroupOffsets :+ consumeAfterConsumerGroupOffset

      case GetKafkaConnection =>
        sender ! this.kafkaConnection

      case GetConsumeAfterConsumerGroupOffsets =>
        sender ! this.consumeAfterConsumerGroupOffsets

      case GetLastConsumeAfterConsumerGroupOffset =>
        sender ! this.consumeAfterConsumerGroupOffsets.lastOption
    }
  }
}

class ConsumerGroupTopicPartitionStatusActorSpec extends TestKit(ActorSystem("MySpec"))
  with ImplicitSender
  with WordSpecLike
  with Matchers
  with BeforeAndAfterAll
  with MockFactory {

  import ConsumerGroupTopicPartitionStatusActor._
  import ConsumerGroupTopicPartitionStatusActorSpec._

  private val prod = KafkaClusterEntityId("prod")
  private val consumerGroupTopicPartition = ConsumerGroupTopicPartition("test-consumer-group", new TopicPartition("test", 0))

  private val defaultKafkaClusterId = prod
  private val defaultConsumerGroupTopicPartition = consumerGroupTopicPartition

  private def props(
    observer: ConsumerGroupMetricsObserver,
    consumerGroupOffsetMode: ConsumerGroupOffsetMode,
    nowFunc: () => OffsetDateTime,
    kafkaClusterId: KafkaClusterEntityId = defaultKafkaClusterId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition = defaultConsumerGroupTopicPartition,
    slidingWindowSize: Duration = 10.minutes,
    consumeAfterConsumerGroupOffsetEnabled: Boolean = true,
    consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long = 100L,
    consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName] = Seq.empty,
    consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName] = Seq.empty
  ): Props = ConsumerGroupTopicPartitionStatusActor.props(
    kafkaClusterId,
    consumerGroupTopicPartition,
    ConsumerGroupStatusEvaluatorConfig(consumerGroupOffsetMode, slidingWindowSize, 0.5),
    observer,
    consumeAfterConsumerGroupOffsetEnabled,
    consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix,
    consumeAfterConsumerGroupOffsetExcludeTopics,
    consumeAfterConsumerGroupOffsetExcludeConsumerGroups,
    nowFunc
  )

  private def name(
    kafkaClusterId: KafkaClusterEntityId = defaultKafkaClusterId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition = defaultConsumerGroupTopicPartition
  ): String = ConsumerGroupTopicPartitionStatusActor.name(kafkaClusterId, consumerGroupTopicPartition)

  private def actorOf(
    observer: ConsumerGroupMetricsObserver,
    consumerGroupOffsetMode: ConsumerGroupOffsetMode,
    nowFunc: () => OffsetDateTime,
    kafkaClusterId: KafkaClusterEntityId = defaultKafkaClusterId,
    consumerGroupTopicPartition: ConsumerGroupTopicPartition = defaultConsumerGroupTopicPartition,
    slidingWindowSize: Duration = 10.minutes,
    consumeAfterConsumerGroupOffsetEnabled: Boolean = true,
    consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix: Long = 100L,
    consumeAfterConsumerGroupOffsetExcludeTopics: Seq[FlexibleName] = Seq.empty,
    consumeAfterConsumerGroupOffsetExcludeConsumerGroups: Seq[FlexibleName] = Seq.empty
  ): ActorRef = system.actorOf(
    props(
      observer,
      consumerGroupOffsetMode,
      nowFunc,
      kafkaClusterId,
      consumerGroupTopicPartition,
      slidingWindowSize,
      consumeAfterConsumerGroupOffsetEnabled,
      consumeAfterConsumerGroupOffsetMaxTopicPartitionLagToFix,
      consumeAfterConsumerGroupOffsetExcludeTopics,
      consumeAfterConsumerGroupOffsetExcludeConsumerGroups
    ),
    name(kafkaClusterId, consumerGroupTopicPartition)
  )

  private def consumeAfterConsumerGroupOffsetActorOf(): ActorRef =
    system.actorOf(Props(new MockConsumeAfterConsumerGroupOffsetActor()))

  private def currentTime(initial: OffsetDateTime = OffsetDateTime.now(Clock.systemUTC)) = new CurrentTime(initial)

  private def stubConsumerGroupMetricsObserver: ConsumerGroupMetricsObserver = {
    val o = stub[ConsumerGroupMetricsObserver]
//    (o.removeConsumerGroupMetrics _).when(*, *).returns(())
//    (o.updateConsumerGroupMetrics _).when(*, *, *).returns(())
    o
  }

  private def wait(duration: FiniteDuration): Unit = {
    Thread.sleep(duration.toMillis)
  }

  private def consumeAfterConsumerGroupOffset(consumerGroupOffset: Long, endOffset: Long): ConsumeAfterConsumerGroupOffset =
    ConsumeAfterConsumerGroupOffset(
      consumerGroupTopicPartition.topicPartition,
      consumerGroupTopicPartition.consumerGroupId,
      consumerGroupOffset,
      endOffset
    )

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "a ConsumerGroupTopicPartitionStatusActor" when {
    "a single PartitionHighOffset is sent" should {
      "send back a metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val partitionHighOffset = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset.withZeroLowOffset
        val expectedMetrics = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionHighOffset), None, None, None)
        expectMsg(expectedMetrics)

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics).once()
      }
    }

    "a single ConsumerGroupOffset is sent" should {
      "send back a metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val consumerGroupOffset = ConsumerGroupOffset(1, "", ct.now)
        actor ! consumerGroupOffset
        val expectedMetrics = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset), None, None)
        expectMsg(expectedMetrics)

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics).once()
      }
    }

    "multiple ConsumerGroupOffset are sent" should {
      "send back metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
        actor ! consumerGroupOffset1
        val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
        expectMsg(expectedMetrics1)

        ct.plusMinutes(5)
        val consumerGroupOffset2 = ConsumerGroupOffset(300, "", ct.now)
        actor ! consumerGroupOffset2
        val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Warning, None, Some(consumerGroupOffset2), None, Some(1.0))
        expectMsg(expectedMetrics2)

        ct.plusMinutes(1)
        val consumerGroupOffset3 = ConsumerGroupOffset(1800, "", ct.now)
        actor ! consumerGroupOffset3
        val expectedMetrics3 = ConsumerGroupMetrics(ConsumerGroupStatus.Warning, None, Some(consumerGroupOffset3), None, Some(5.0))
        expectMsg(expectedMetrics3)

        ct.plusMinutes(5)
        val consumerGroupOffset4 = ConsumerGroupOffset(2100, "", ct.now)
        actor ! consumerGroupOffset4
        val expectedMetrics4 = ConsumerGroupMetrics(ConsumerGroupStatus.Warning, None, Some(consumerGroupOffset4), None, Some(5))
        expectMsg(expectedMetrics4)

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics3).once()
      }
    }


    "a single PartitionHighOffset and then a single ConsumerGroupOffset is sent" should {
      "send back two metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val consumerGroupOffset = ConsumerGroupOffset(1, "", ct.now)
        ct.plusMinutes(1)
        actor ! consumerGroupOffset
        val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset), None, None)
        expectMsg(expectedMetrics1)

        ct.plusMinutes(2)
        val partitionHighOffset = PartitionHighOffset(3, ct.now)
        actor ! partitionHighOffset.withZeroLowOffset
        val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionHighOffset), Some(consumerGroupOffset), Some(2), None)
        expectMsg(expectedMetrics2)

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()
      }
    }

    "a few PartitionHighOffsets are sent and the some scrolls out of the window" should {
      "send back all the metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val partitionHighOffset1 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset1.withZeroLowOffset
        val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics1)

        ct.plusMinutes(5)
        val partitionHighOffset2 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset2.withZeroLowOffset
        val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics2)

        ct.plusMinutes(6)
        val partitionHighOffset3 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset3.withZeroLowOffset
        val expectedMetrics3 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics3)

        actor ! GetOffsetsWindow
        expectMsg(Seq(
          Offsets(partitionHighOffset2.lastOffsetsTimestampUtc, None, None, Some(partitionHighOffset1.withZeroLowOffset)),
          Offsets(partitionHighOffset3.lastOffsetsTimestampUtc, None, None, Some(partitionHighOffset1.withZeroLowOffset))
        ))

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).twice()
        // expectedMetrics2 is the same as expectedMetrics3, hence twice() above
      }
    }

    "a few PartitionHighOffsets are sent but none scrolls out of the window because it's larger" should {
      "send back all the metrics" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

        val partitionHighOffset1 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset1.withZeroLowOffset
        val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics1)

        // making the window 20 minutes from the default 5
        actor ! ConsumerGroupMetricsConfig(slidingWindowSize = 20.minutes.some)

        ct.plusMinutes(5)
        val partitionHighOffset2 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset2.withZeroLowOffset
        val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics2)

        ct.plusMinutes(6)
        val partitionHighOffset3 = PartitionHighOffset(1, ct.now)
        actor ! partitionHighOffset3.withZeroLowOffset
        val expectedMetrics3 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionHighOffset1), None, None, None)
        expectMsg(expectedMetrics3)

        actor ! GetOffsetsWindow
        expectMsg(Seq(
          Offsets(partitionHighOffset1.lastOffsetsTimestampUtc, None, None, Some(partitionHighOffset1.withZeroLowOffset)),
          Offsets(partitionHighOffset2.lastOffsetsTimestampUtc, None, None, Some(partitionHighOffset1.withZeroLowOffset)),
          Offsets(partitionHighOffset3.lastOffsetsTimestampUtc, None, None, Some(partitionHighOffset1.withZeroLowOffset))
        ))

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).twice()
        // expectedMetrics2 is the same as expectedMetrics1, hence twice() above
        (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics3).once()
      }
    }

    "a SetConsumeAfterConsumerGroupOffsetActor is sent" should {
      "store the actor and clears it up when it terminates" in {
        val o = stubConsumerGroupMetricsObserver
        val ct = currentTime()
        val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)
        val consumeAfterConsumerGroupOffsetActor = consumeAfterConsumerGroupOffsetActorOf()

        actor ! SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor)
        actor ! GetConsumeAfterConsumerGroupOffsetActor
        expectMsg(Some(consumeAfterConsumerGroupOffsetActor))

        expectNoMessage()

        consumeAfterConsumerGroupOffsetActor ! PoisonPill
        wait(1.second) // to make sure the actor received the notification about the termination
        actor ! GetConsumeAfterConsumerGroupOffsetActor
        expectMsg(None)

        expectNoMessage()

        (o.removeConsumerGroupMetrics _).verify(*, *).never()
      }
    }
  }

  "the committed offset is behind the partition's low-offset" should {
    "fast-forward it to the low-offset" in {
      val o = stubConsumerGroupMetricsObserver
      val ct = currentTime()
      val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)

      val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
      actor ! consumerGroupOffset1
      val expectedMetrics0 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
      expectMsg(expectedMetrics0)

      val partitionLowHighOffset1 = PartitionLowHighOffset(0, 10, ct.now)
      actor ! partitionLowHighOffset1
      val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, Some(partitionLowHighOffset1.partitionHighOffset), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics1)

      ct.plusMinutes(5)
      val partitionLowHighOffset2 = PartitionLowHighOffset(0, 11, ct.now)
      actor ! partitionLowHighOffset2
      val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionLowHighOffset2.partitionHighOffset), Some(consumerGroupOffset1), Some(11), None)
      expectMsg(expectedMetrics2)

      ct.plusMinutes(1)
      // suddenly kafka decided to clean-up the topic, maybe retention kicked in, so low-offset = high-offset = 11
      // in this case we expect the status to be OK, but leave the lag of 11 there as a FYI, and also the consumption rate doesn't change (it wasn't really consumption)
      val partitionLowHighOffset3 = PartitionLowHighOffset(11, 11, ct.now)
      actor ! partitionLowHighOffset3
      val expectedMetrics3 = ConsumerGroupMetrics(ConsumerGroupStatus.Ok, Some(partitionLowHighOffset3.partitionHighOffset), Some(consumerGroupOffset1), Some(11), None)
      expectMsg(expectedMetrics3)

      actor ! GetOffsetsWindow
      expectMsg(Seq(
        Offsets(consumerGroupOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, None),
        Offsets(partitionLowHighOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, Some(partitionLowHighOffset1)),
        Offsets(partitionLowHighOffset2.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, Some(partitionLowHighOffset2)),
        Offsets(partitionLowHighOffset3.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), Some(consumerGroupOffset1.copy(offset = 11)), Some(partitionLowHighOffset3))
      ))

      expectNoMessage()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics0).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics3).once()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
    }
  }

  "there are no real messages after the committed offset" should {
    "fast-forward it to the end-offset" in {
      val o = stubConsumerGroupMetricsObserver
      val ct = currentTime()
      val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)
      val consumeAfterConsumerGroupOffsetActor = consumeAfterConsumerGroupOffsetActorOf()

      // setting the dummy consume-after-consumer-group-offset actor
      actor ! SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor)
      actor ! GetConsumeAfterConsumerGroupOffsetActor
      expectMsg(Some(consumeAfterConsumerGroupOffsetActor))

      // initialize some lag
      val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
      actor ! consumerGroupOffset1
      val expectedMetrics0 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
      expectMsg(expectedMetrics0)

      ct.plusMinutes(5)
      val partitionLowHighOffset1 = PartitionLowHighOffset(0, 10, ct.now)
      actor ! partitionLowHighOffset1
      val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionLowHighOffset1.partitionHighOffset), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics1)

      // expecting that the actor requested the consumeAfterConsumerGroupOffsetActor to consume
      val consumeAfterConsumerGroupOffsetRequest = consumeAfterConsumerGroupOffset(consumerGroupOffset = 0, endOffset = 10)
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(Some(consumeAfterConsumerGroupOffsetRequest))

      consumeAfterConsumerGroupOffsetActor ! GetConsumeAfterConsumerGroupOffsets
      expectMsg(Seq(consumeAfterConsumerGroupOffsetRequest))
      // simulating a response where there are no real messages until the end-offset
      val expectedMetrics2TimestampUtc = ct.plusSeconds(1).now
      val consumeAfterConsumerGroupOffsetResult = ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(consumeAfterConsumerGroupOffsetRequest, from = 0, to = 10, messageAtTo = false)
      actor ! consumeAfterConsumerGroupOffsetResult

      // which should result in an adjusted metric
      val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Ok, Some(partitionLowHighOffset1.partitionHighOffset), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics2)

      // and we expect the actor to store the result for later use
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(Some(consumeAfterConsumerGroupOffsetResult))

      actor ! GetOffsetsWindow
      expectMsg(Seq(
        Offsets(consumerGroupOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, None),
        Offsets(partitionLowHighOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, Some(partitionLowHighOffset1)),
        Offsets(expectedMetrics2TimestampUtc, Some(consumerGroupOffset1), Some(consumerGroupOffset1.copy(offset = 10)), Some(partitionLowHighOffset1)),
      ))

      expectNoMessage()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics0).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
    }
  }

  "there are real messages after the committed offset but not directly after" should {
    "fast-forward it to the first real message" in {
      val o = stubConsumerGroupMetricsObserver
      val ct = currentTime()
      val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)
      val consumeAfterConsumerGroupOffsetActor = consumeAfterConsumerGroupOffsetActorOf()

      // setting the dummy consume-after-consumer-group-offset actor
      actor ! SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor)
      actor ! GetConsumeAfterConsumerGroupOffsetActor
      expectMsg(Some(consumeAfterConsumerGroupOffsetActor))

      // initialize some lag
      val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
      actor ! consumerGroupOffset1
      val expectedMetrics0 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
      expectMsg(expectedMetrics0)

      ct.plusMinutes(5)
      val partitionLowHighOffset1 = PartitionLowHighOffset(0, 10, ct.now)
      actor ! partitionLowHighOffset1
      val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionLowHighOffset1.partitionHighOffset), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics1)

      // expecting that the actor requested the consumeAfterConsumerGroupOffsetActor to consume
      val consumeAfterConsumerGroupOffsetRequest = consumeAfterConsumerGroupOffset(consumerGroupOffset = 0, endOffset = 10)
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(Some(consumeAfterConsumerGroupOffsetRequest))

      consumeAfterConsumerGroupOffsetActor ! GetConsumeAfterConsumerGroupOffsets
      expectMsg(Seq(consumeAfterConsumerGroupOffsetRequest))
      // simulating a response where there are no real messages until the end-offset
      val expectedMetrics2TimestampUtc = ct.plusSeconds(1).now
      val consumeAfterConsumerGroupOffsetResult = ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(consumeAfterConsumerGroupOffsetRequest, from = 0, to = 7, messageAtTo = true)
      actor ! consumeAfterConsumerGroupOffsetResult

      // which should result in an adjusted metric
      expectMsg(expectedMetrics1)

      // and we expect the actor to store the result for later use
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(Some(consumeAfterConsumerGroupOffsetResult))

      actor ! GetOffsetsWindow
      expectMsg(Seq(
        Offsets(consumerGroupOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, None),
        Offsets(partitionLowHighOffset1.lastOffsetsTimestampUtc, Some(consumerGroupOffset1), None, Some(partitionLowHighOffset1)),
        Offsets(expectedMetrics2TimestampUtc, Some(consumerGroupOffset1), Some(consumerGroupOffset1.copy(offset = 7)), Some(partitionLowHighOffset1)),
      ))

      expectNoMessage()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics0).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).twice()
    }
  }

  "two PartitionHighOffset are sent, the second after a topic re-creation" should {
    "reset the pending consumeAfterConsumerGroupOffsets" in {
      val o = stubConsumerGroupMetricsObserver
      val ct = currentTime()
      val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)
      val consumeAfterConsumerGroupOffsetActor = consumeAfterConsumerGroupOffsetActorOf()

      // setting the dummy consume-after-consumer-group-offset actor
      actor ! SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor)
      actor ! GetConsumeAfterConsumerGroupOffsetActor
      expectMsg(Some(consumeAfterConsumerGroupOffsetActor))

      // initialize some lag
      val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
      actor ! consumerGroupOffset1
      val expectedMetrics0 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
      expectMsg(expectedMetrics0)

      ct.plusMinutes(5)
      val partitionHighOffset1 = PartitionHighOffset(10, ct.now)
      actor ! partitionHighOffset1.withZeroLowOffset
      val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionHighOffset1), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics1)

      // expecting that the actor requested the consumeAfterConsumerGroupOffsetActor to consume
      val consumeAfterConsumerGroupOffsetRequest = consumeAfterConsumerGroupOffset(consumerGroupOffset = 0, endOffset = 10)
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(Some(consumeAfterConsumerGroupOffsetRequest))

      ct.plusSeconds(1)
      // the topic has been re-created so the high offset is back to zero
      val partitionHighOffset2 = PartitionHighOffset(0, ct.now)
      actor ! partitionHighOffset2.withZeroLowOffset
      val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Ok, Some(partitionHighOffset2), Some(consumerGroupOffset1), Some(0), None)
      expectMsg(expectedMetrics2)

      // both pending and result consumeAfterConsumerGroupOffset must be empty after topic-recreation
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(None)
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(None)

      // simulating the consume response back to the actor (pending is gone now so it should be ignored)
      val consumeAfterConsumerGroupOffsetResult = ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(consumeAfterConsumerGroupOffsetRequest, from = 0, to = 10, messageAtTo = false)
      actor ! consumeAfterConsumerGroupOffsetResult
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(None)

      expectNoMessage()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics0).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()
    }
  }

  "two PartitionHighOffset are sent, the second after a topic re-creation" should {
    "reset the result consumeAfterConsumerGroupOffsets" in {
      val o = stubConsumerGroupMetricsObserver
      val ct = currentTime()
      val actor = actorOf(o, ConsumerGroupOffsetMode.ActualCommit, ct.nowFunc)
      val consumeAfterConsumerGroupOffsetActor = consumeAfterConsumerGroupOffsetActorOf()

      // setting the dummy consume-after-consumer-group-offset actor
      actor ! SetConsumeAfterConsumerGroupOffsetActor(consumeAfterConsumerGroupOffsetActor)
      actor ! GetConsumeAfterConsumerGroupOffsetActor
      expectMsg(Some(consumeAfterConsumerGroupOffsetActor))

      // initialize some lag
      val consumerGroupOffset1 = ConsumerGroupOffset(0, "", ct.now)
      actor ! consumerGroupOffset1
      val expectedMetrics0 = ConsumerGroupMetrics(ConsumerGroupStatus.Unknown, None, Some(consumerGroupOffset1), None, None)
      expectMsg(expectedMetrics0)

      ct.plusMinutes(5)
      val partitionHighOffset1 = PartitionHighOffset(10, ct.now)
      actor ! partitionHighOffset1.withZeroLowOffset
      val expectedMetrics1 = ConsumerGroupMetrics(ConsumerGroupStatus.Error, Some(partitionHighOffset1), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics1)

      // expecting that the actor requested the consumeAfterConsumerGroupOffsetActor to consume
      val consumeAfterConsumerGroupOffsetRequest = consumeAfterConsumerGroupOffset(consumerGroupOffset = 0, endOffset = 10)
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(Some(consumeAfterConsumerGroupOffsetRequest))

      consumeAfterConsumerGroupOffsetActor ! GetConsumeAfterConsumerGroupOffsets
      expectMsg(Seq(consumeAfterConsumerGroupOffsetRequest))
      // simulating a response where there are no real messages until the end-offset
      ct.plusSeconds(1).now
      val consumeAfterConsumerGroupOffsetResult = ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(consumeAfterConsumerGroupOffsetRequest, from = 0, to = 10, messageAtTo = false)
      actor ! consumeAfterConsumerGroupOffsetResult

      // which should result in an adjusted metric
      val expectedMetrics2 = ConsumerGroupMetrics(ConsumerGroupStatus.Ok, Some(partitionHighOffset1), Some(consumerGroupOffset1), Some(10), None)
      expectMsg(expectedMetrics2)

      // and we expect the actor to store the result for later use
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(Some(consumeAfterConsumerGroupOffsetResult))

      ct.plusMinutes(1)
      // the topic has been re-created so the high offset is back to zero
      val partitionHighOffset3 = PartitionHighOffset(0, ct.now)
      actor ! partitionHighOffset3.withZeroLowOffset
      val expectedMetrics3 = ConsumerGroupMetrics(ConsumerGroupStatus.Ok, Some(partitionHighOffset3), Some(consumerGroupOffset1), Some(0), None)
      expectMsg(expectedMetrics3)

      // both pending and result consumeAfterConsumerGroupOffset must be empty after topic-recreation
      actor ! GetPendingConsumeAfterConsumerGroupOffset
      expectMsg(None)
      actor ! GetConsumeAfterConsumerGroupOffsetResult
      expectMsg(None)

      expectNoMessage()

      (o.removeConsumerGroupMetrics _).verify(*, *).never()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics0).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics1).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics2).once()
      (o.updateConsumerGroupMetrics _).verify(defaultKafkaClusterId, defaultConsumerGroupTopicPartition, expectedMetrics3).once()
    }
  }

  // TODO testing that it requests consumption at the right time
}
