/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import KafkaConsumerUtilsTest.test
import com.mwam.kafkakewl.common.kafka.KafkaConsumerUtils
import org.apache.kafka.common.TopicPartition
import zio.*
import zio.kafka.consumer.{Consumer}
import zio.kafka.producer.{Producer}
import zio.kafka.serde.{Deserializer}
import zio.kafka.testkit.KafkaTestUtils.{consumer, produceMany, producer}
import zio.kafka.testkit.*
import zio.test.*

object KafkaConsumerUtilsTest extends ZIOSpecWithKafka with KafkaRandom {
  override def kafkaPrefix: String = "consumer-spec"

  override def spec: Spec[TestEnvironment & Kafka & Scope, Any] =
    suite("KafkaConsumerUtils test suite")(
      test("non-empty topic") {
        val kvs: List[(String, String)] = (1 to 5).toList.map(i => (s"key-$i", s"msg-$i"))
        for {
          topic <- randomTopic
          _ <- produceMany(topic, kvs)
          cons <- ZIO.service[Consumer]
          tps <- cons.partitionsFor(topic).map(tpis => tpis.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())))

          recordsAndTpos <- KafkaConsumerUtils
            .consumeUntilEnd(cons, tps, Deserializer.string, Deserializer.string)

          (topicPartitionOffsets, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
          _ <- ZIO.debug(s"Offsets: ${topicPartitionOffsets}")
        } yield assertTrue(records.length == 5, topicPartitionOffsets.values.sum >= 5L)
      },
      test("empty topic") {
        for {
          topic <- randomTopic
          cons <- ZIO.service[Consumer]
          tps <- cons.partitionsFor(topic).map(tpis => tpis.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())))
          recordsAndTpos <- KafkaConsumerUtils
            .consumeUntilEnd(cons, tps, Deserializer.string, Deserializer.string)
          (topicPartitionOffsets, records) = recordsAndTpos
          _ <- cons.stopConsumption
          _ <- ZIO.debug(s"Number of records: ${records.length}")
          _ <- ZIO.debug(s"Offsets: ${topicPartitionOffsets}")
        } yield assertTrue(records.isEmpty, topicPartitionOffsets.values.sum >= 0L)
      },
      test("large topic") {
        val kvs: List[(String, String)] = (1 to 1000000).toList.map(i => (s"key-$i", s"msg-$i"))
        for {
          topic <- randomTopic
          _ <- produceMany(topic, kvs)
          cons <- ZIO.service[Consumer]
          tps <- cons.partitionsFor(topic).map(tpis => tpis.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())))

          recordsAndTpos <- KafkaConsumerUtils
            .consumeUntilEnd(cons, tps, Deserializer.string, Deserializer.string)

          (topicPartitionOffsets, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
          _ <- ZIO.debug(s"Offsets: ${topicPartitionOffsets}")
        } yield assertTrue(records.length == 1000000, topicPartitionOffsets.values.sum >= 1000000L)
      }
    )
      .provideSome[Kafka](
        producer,
        ZLayer.fromZIOEnvironment(randomClient.zip(randomGroup).flatMap((c, g) => consumer(c, Some(g)).build)),
        Scope.default
      )
      .provideSomeShared(
        Kafka.embedded
      ) @@ TestAspect.withLiveClock

}
