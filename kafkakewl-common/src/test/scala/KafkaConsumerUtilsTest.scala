/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

import KafkaConsumerUtilsTest.test
import com.mwam.kafkakewl.common.kafka.KafkaConsumerUtils
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import zio.*
import zio.kafka.consumer.{CommittableRecord, Consumer, ConsumerSettings, Subscription}
import zio.kafka.producer.TransactionalProducer.UserInitiatedAbort
import zio.kafka.producer.{Producer, TransactionalProducer}
import zio.kafka.serde.{Deserializer, Serde}
import zio.kafka.testkit.KafkaTestUtils.{consumer, produceMany, producer, transactionalConsumerSettings, transactionalProducer}
import zio.kafka.testkit.*
import zio.stream.Take
import zio.test.*

object KafkaConsumerUtilsTest extends ZIOSpecWithKafka with KafkaRandom {
  def withConsumerInt(
      subscription: Subscription,
      settings: ConsumerSettings
  ): ZIO[Any with Scope, Throwable, Dequeue[Take[Throwable, CommittableRecord[String, Int]]]] =
    Consumer.make(settings).flatMap { c =>
      c.plainStream(subscription, Serde.string, Serde.int).toQueue()
    }
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
      },
      test("rolledback query") {

        for {
          topic <- randomTopic
          group <- randomGroup
          client <- randomClient
          kvs = (1 to 100).toList.map(i => new ProducerRecord(topic, s"key-$i", s"msg-$i"))

          _ <- ZIO.scoped {
            TransactionalProducer.createTransaction.tap { t =>
              ZIO.foreach(kvs.take(10))(t.produce(_, Serde.string, Serde.string, None))
            }
          }
          _ <- ZIO
            .scoped {
              TransactionalProducer.createTransaction
                .flatMap { t =>
                  ZIO.foreachDiscard(kvs.take(50))(t.produce(_, Serde.string, Serde.string, None)) *> t.abort
                }
            }
            .catchSome { case UserInitiatedAbort =>
              ZIO.unit
            }
          _ <- ZIO.scoped {
            TransactionalProducer.createTransaction.tap { t =>
              ZIO.foreach(kvs.take(10))(t.produce(_, Serde.string, Serde.string, None))
            }
          }
          recordsAndTpos <- ZIO.scoped {
            for {
              settings <- transactionalConsumerSettings(group, client)
              cons <- Consumer.make(settings)
              tps <- cons.partitionsFor(topic).map(tpis => tpis.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())))

              recordsAndTpos <- KafkaConsumerUtils
                .consumeUntilEnd(cons, tps, Serde.string, Serde.string)

            } yield recordsAndTpos
          }

          (topicPartitionOffsets, records) = recordsAndTpos
          _ <- ZIO.debug(s"Number of records: ${records.length}")
          _ <- ZIO.debug(s"Offsets: ${topicPartitionOffsets}")
        } yield assertTrue(records.length == 20, topicPartitionOffsets.values.sum >= 70L)

      }
    )
      .provideSome[Kafka](
        producer,
        ZLayer.fromZIOEnvironment(randomClient.zip(randomGroup).flatMap((c, g) => consumer(c, Some(g)).build)),
        Scope.default,
        transactionalProducer,
        Kafka.embedded
      ) @@ TestAspect.withLiveClock

}
