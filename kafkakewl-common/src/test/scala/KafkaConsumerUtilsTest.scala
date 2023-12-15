/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import com.mwam.kafkakewl.common.kafka.KafkaConsumerUtils
import org.apache.kafka.common.TopicPartition
import zio.*
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.kafka.serde.{Deserializer}
import zio.kafka.testkit.KafkaTestUtils.{consumerSettings, minimalConsumer, produceMany, producer}
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

          (_, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
        } yield assertTrue(records.length == 5)
      },
      test("empty topic") {
        for {
          topic <- randomTopic
          cons <- ZIO.service[Consumer]
          tps <- cons.partitionsFor(topic).map(tpis => tpis.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())))

          recordsAndTpos <- KafkaConsumerUtils
            .consumeUntilEnd(cons, tps, Deserializer.string, Deserializer.string)

          (_, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
        } yield assertTrue(records.isEmpty)
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

          (_, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
        } yield assertTrue(records.length == 1000000)
      }
    )
      .provideSome[Kafka](
        producer,
        minimalConsumer(),
        ZLayer.fromZIO(randomClient.zip(randomGroup).flatMap((c, g) => consumerSettings(c, Some(g))))
      )
      .provideSomeShared(
        Kafka.embedded
      ) @@ TestAspect.withLiveClock

}
