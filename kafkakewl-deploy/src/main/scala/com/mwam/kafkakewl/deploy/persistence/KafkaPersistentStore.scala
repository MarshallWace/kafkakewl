/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.persistence

import com.mwam.kafkakewl.common.kafka.KafkaConsumerExtensions.*
import com.mwam.kafkakewl.common.kafka.KafkaConsumerUtils
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.DeploymentsJson.given
import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import org.apache.kafka.clients.consumer.Consumer
import zio.*
import zio.json.*
import zio.kafka.producer.Producer
import zio.kafka.serde.Serde

import scala.collection.mutable.ListBuffer

class KafkaPersistentStore(private val kafkaClientConfig: KafkaClientConfig, kafkaProducer: Producer) extends PersistentStore {
  // TODO make these configurable
  private val TOPIC = "kewltest.vnext.persistent-store"

  private object KafkaSerde {
    val key: Serde[Any, TopologyId] =
      Serde.string.inmapM(
        s => ZIO.succeed(TopologyId(s))
      )(
        topologyId => ZIO.succeed(topologyId.value)
      )
    val value: Serde[Any, TopologyDeployment] =
      Serde.string.inmapM[Any, TopologyDeployment](
        s => ZIO.fromEither(s.fromJson[TopologyDeployment]).mapError(e => new RuntimeException(e))
      )(
        td => ZIO.succeed(td.toJson)
      )
  }

  override def loadAll(): Task[Seq[TopologyDeployment]] =
    ZIO.scoped {
      for {
        consumer <- KafkaConsumerUtils.kafkaConsumerStringStringZIO(kafkaClientConfig)
        _ <- ZIO.logInfo(s"loading the topologies from the $TOPIC kafka topic")
        deserializedTopologiesWithDuration <- consumeUntilEnd(consumer, TOPIC).timed
        (duration, deserializedTopologies) = deserializedTopologiesWithDuration
        failedTopologies = deserializedTopologies
          .collect { case (id, Left(jsonError)) => ZIO.logError(s"failed to deserialize topology $id: $jsonError")}
          .toList
        topologies = deserializedTopologies.collect { case (_, Right(topology)) => topology }.toList

        _ <- ZIO.foreachDiscard(failedTopologies)(identity)
        _ <- ZIO.logInfo(s"finished loading ${topologies.size} topologies, discarded ${failedTopologies.size} from the $TOPIC kafka topic in ${duration.toMillis / 1000.0} seconds")
      } yield topologies
    }

  override def save(topologyDeployments: Map[TopologyId, TopologyDeployment]): Task[Unit] =
    ZIO.foreach(topologyDeployments.values) { td =>
      for {
        recordMetadataWithDuration <- kafkaProducer.produce(TOPIC, td.topologyId, td, KafkaSerde.key, KafkaSerde.value).timed
        (duration, recordMetadata) = recordMetadataWithDuration
        _ <- ZIO.logInfo(s"saved topology ${td.topologyId} into $TOPIC kafka topic: P#${recordMetadata.partition} @${recordMetadata.offset} in ${duration.toMillis / 1000.0} seconds")
      } yield ()
    }.unit

  private def consumeUntilEnd(consumer: Consumer[String, String], topic: String): Task[Map[TopologyId, Either[String, TopologyDeployment]]] =
    ZIO.attempt {
      val messages = new ListBuffer[(String, String)]()
      val topicPartitions = consumer.topicPartitionsOf(topic)
      KafkaConsumerUtils.consumeUntilEnd(consumer, topicPartitions) { _.forEach(cr => messages += (cr.key -> cr.value)) }

      messages
        .map { case (key, value) => (TopologyId(key), value.fromJson[TopologyDeployment]) }
        .toMap
    }
}

object KafkaPersistentStore {
  val live: ZLayer[KafkaClientConfig & Producer, Nothing, KafkaPersistentStore] =
    ZLayer.fromFunction(KafkaPersistentStore(_, _))
}