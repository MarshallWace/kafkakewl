package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.metrics.domain.{
  ConsumerGroupStatus,
  ConsumerGroupTopicPartition,
  KafkaConsumerGroupMetrics,
  KafkaConsumerGroupOffset,
  KafkaConsumerGroupOffsets,
  KafkaTopicPartition,
  KafkaTopicPartitionInfo,
  KafkaTopicPartitionInfoChanges
}
import com.mwam.kafkakewl.metrics.services.KafkaConsumerMetricsCalcTest.test
import zio.stream.ZStream
import zio.{Duration, Queue, UIO, ZIO}
import zio.test.{Spec, TestAspect, ZIOSpecDefault, assertTrue}

import java.time.{Instant, OffsetDateTime}

object KafkaConsumerMetricsCalcTest extends ZIOSpecDefault {

  def getConsumerMetricsCalc(
      topicQueue: Queue[KafkaTopicPartitionInfoChanges],
      consumerInfoQueue: Queue[KafkaConsumerGroupOffsets]
  ): UIO[ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges]] =
    KafkaConsumerGroupMetricsCalc.createConsumerGroupMetricStream(ZStream.fromQueue(consumerInfoQueue), ZStream.fromQueue(topicQueue))

  def getQueuesAndMetricsCalc: ZIO[
    Any,
    Nothing,
    (Queue[KafkaTopicPartitionInfoChanges], Queue[KafkaConsumerGroupOffsets], ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges])
  ] = for {
    topicQueue <- Queue.bounded[KafkaTopicPartitionInfoChanges](1)
    consumerQueue <- Queue.bounded[KafkaConsumerGroupOffsets](1)
    cmc <- getConsumerMetricsCalc(topicQueue, consumerQueue)
  } yield (topicQueue, consumerQueue, cmc)

  def pushTestConsumerGroup(
      cmc: ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges],
      consumerQueue: Queue[KafkaConsumerGroupOffsets]
  ): UIO[KafkaConsumerGroupMetricChanges] = for {
    _ <- consumerQueue.offer(
      Map(ConsumerGroupTopicPartition("testGroup", "testTopic", 0) -> Some(KafkaConsumerGroupOffset(123, "", OffsetDateTime.MAX)))
    )
    result <- cmc.take(1).runCollect
  } yield result(0)

  def pushTestTopic(
      cmc: ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges],
      topicQueue: Queue[KafkaTopicPartitionInfoChanges]
  ): UIO[KafkaConsumerGroupMetricChanges] = for {
    _ <- topicQueue.offer(
      KafkaTopicPartitionInfoChanges(
        Map(KafkaTopicPartition("testTopic", 0) -> KafkaTopicPartitionInfo(20, 3, Instant.MAX)),
        Set.empty
      )
    )
    result <- cmc.take(1).runCollect
  } yield result(0)

  def deleteTestConsumerGroup(
      cmc: ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges],
      consumerQueue: Queue[KafkaConsumerGroupOffsets]
  ): UIO[KafkaConsumerGroupMetricChanges] = for {
    _ <- consumerQueue
      .offer(
        Map(ConsumerGroupTopicPartition("testGroup", "testTopic", 0) -> None)
      )
    result <- cmc.take(1).runCollect
  } yield result(0)

  def deleteTestTopic(
      cmc: ZStream[Any, Nothing, KafkaConsumerGroupMetricChanges],
      topicQueue: Queue[KafkaTopicPartitionInfoChanges]
  ): UIO[KafkaConsumerGroupMetricChanges] = for {
    _ <- topicQueue
      .offer(
        KafkaTopicPartitionInfoChanges(
          Map.empty,
          Set(KafkaTopicPartition("testTopic", 0))
        )
      )
    result <- cmc.take(1).runCollect
  } yield result(0)

  def kafkaConsumerGroupMetricChangesWithoutTopic: KafkaConsumerGroupMetricChanges = KafkaConsumerGroupMetricChanges(
    Map(
      ConsumerGroupTopicPartition("testGroup", "testTopic", 0) -> KafkaConsumerGroupMetrics(
        ConsumerGroupStatus.Ok,
        None,
        Some(KafkaConsumerGroupOffset(123, "", OffsetDateTime.MAX)),
        None,
        Some(0.0)
      )
    ),
    Set.empty
  )

  def kafkaConsumerGroupMetricChangesWithTopic: KafkaConsumerGroupMetricChanges = KafkaConsumerGroupMetricChanges(
    Map(
      ConsumerGroupTopicPartition("testGroup", "testTopic", 0) -> KafkaConsumerGroupMetrics(
        ConsumerGroupStatus.Ok,
        Some(KafkaTopicPartitionInfo(20, 3, Instant.MAX)),
        Some(KafkaConsumerGroupOffset(123, "", OffsetDateTime.MAX)),
        Some(0),
        Some(0.0)
      )
    ),
    Set.empty
  )

  def kafkaConsumerGroupMetricChangesRemoved: KafkaConsumerGroupMetricChanges = KafkaConsumerGroupMetricChanges(
    Map.empty,
    Set(ConsumerGroupTopicPartition("testGroup", "testTopic", 0))
  )

  def spec: Spec[Any, Nothing] = suite("KafkaConsumerMetricsCalc Tests")(
    test("Adding a kafka consumer group starts adding consumer group metrics") {
      for {
        start <- getQueuesAndMetricsCalc
        (_, consumerQueue, cmc) = start
        firstResult <- pushTestConsumerGroup(cmc, consumerQueue)

      } yield assertTrue(
        firstResult == kafkaConsumerGroupMetricChangesWithoutTopic
      )
    } @@ TestAspect.withLiveClock,
    test("Nulling a kafka consumer group removes consumer group metrics") {
      for {
        start <- getQueuesAndMetricsCalc
        (_, consumerQueue, cmc) = start

        firstResult <- pushTestConsumerGroup(cmc, consumerQueue)
        secondResult <- deleteTestConsumerGroup(cmc, consumerQueue)

      } yield assertTrue(
        firstResult == kafkaConsumerGroupMetricChangesWithoutTopic &&
          secondResult == kafkaConsumerGroupMetricChangesRemoved
      )
    } @@ TestAspect.withLiveClock,
    test("Adding a topic then consumer metric with both") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        _ <- topicQueue
          .offer(
            KafkaTopicPartitionInfoChanges(
              Map(KafkaTopicPartition("testTopic", 0) -> KafkaTopicPartitionInfo(20, 3, Instant.MAX)),
              Set.empty
            )
          )

        // As the queue is bounded to 1 we wait until the last finished processing
        _ <- topicQueue.offer(
          KafkaTopicPartitionInfoChanges(
            Map(KafkaTopicPartition("testTopic", 1) -> KafkaTopicPartitionInfo(20, 3, Instant.MAX)),
            Set.empty
          )
        )

        firstResult <- pushTestConsumerGroup(cmc, consumerQueue)

      } yield assertTrue(
        firstResult == kafkaConsumerGroupMetricChangesWithTopic
      )
    } @@ TestAspect.withLiveClock,
    test("Adding a consumer then topic produces two outputs (without then with the topic info)") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        firstResult <- pushTestConsumerGroup(cmc, consumerQueue)
        secondResult <- pushTestTopic(cmc, topicQueue)
      } yield assertTrue(
        firstResult == kafkaConsumerGroupMetricChangesWithoutTopic &&
          secondResult == kafkaConsumerGroupMetricChangesWithTopic
      )
    } @@ TestAspect.withLiveClock,
    test("Removing a topic deletes the metric") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        _ <- pushTestConsumerGroup(cmc, consumerQueue)
        firstResult <- pushTestTopic(cmc, topicQueue)

        secondResult <- deleteTestTopic(cmc, topicQueue)

      } yield assertTrue(
        firstResult == kafkaConsumerGroupMetricChangesWithTopic &&
          secondResult == kafkaConsumerGroupMetricChangesRemoved
      )
    } @@ TestAspect.withLiveClock,
    test("Removing a topic and republishing does nothing") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        _ <- pushTestConsumerGroup(cmc, consumerQueue)
        _ <- pushTestTopic(cmc, topicQueue)

        _ <- deleteTestTopic(cmc, topicQueue)

        neverFinishes <- pushTestTopic(cmc, topicQueue).timeout(Duration.fromSeconds(5))

      } yield assertTrue(
        neverFinishes.isEmpty
      )
    } @@ TestAspect.withLiveClock,
    test("Removing a topic and republishing consumer does nothing") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        _ <- pushTestConsumerGroup(cmc, consumerQueue)
        _ <- pushTestTopic(cmc, topicQueue)

        _ <- deleteTestTopic(cmc, topicQueue)

        neverFinishes <- pushTestConsumerGroup(cmc, consumerQueue).timeout(Duration.fromSeconds(5))

      } yield assertTrue(
        neverFinishes.isEmpty
      )
    } @@ TestAspect.withLiveClock,
    test("Removing a topic and consumer then republishing consumer publishes metrics without topic") {
      for {
        start <- getQueuesAndMetricsCalc
        (topicQueue, consumerQueue, cmc) = start

        _ <- pushTestConsumerGroup(cmc, consumerQueue)
        _ <- pushTestTopic(cmc, topicQueue)

        _ <- deleteTestTopic(cmc, topicQueue)

        _ <- deleteTestConsumerGroup(cmc, consumerQueue)
        result <- pushTestConsumerGroup(cmc, consumerQueue)

      } yield assertTrue(
        result == kafkaConsumerGroupMetricChangesWithoutTopic
      )
    } @@ TestAspect.withLiveClock
  )
}
