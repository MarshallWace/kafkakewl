/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence

import com.mwam.kafkakewl.common.kafka.KafkaClientConfigExtensions.*
import com.mwam.kafkakewl.common.kafka.AdminClientExtensions.*
import com.mwam.kafkakewl.common.kafka.KafkaConsumerUtils
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.DeploymentsJson.given
import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import zio.kafka.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import zio.*
import zio.json.*
import zio.kafka.producer.*
import zio.kafka.consumer.*
import zio.kafka.serde.{Deserializer, Serde}
import zio.kafka.admin.*
import zio.kafka.consumer.Consumer.AutoOffsetStrategy
import zio.stream.*

import scala.collection.mutable.ListBuffer

final case class BatchMessageEnvelope[Payload](
    batchSize: Int,
    indexInBatch: Int,
    payload: Payload
)

object BatchMessageEnvelopeJson {
  import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

  given [Payload](using JsonEncoder[Payload]): JsonEncoder[BatchMessageEnvelope[Payload]] = DeriveJsonEncoder.gen[BatchMessageEnvelope[Payload]]
  given [Payload](using JsonDecoder[Payload]): JsonDecoder[BatchMessageEnvelope[Payload]] = DeriveJsonDecoder.gen[BatchMessageEnvelope[Payload]]
}

class KafkaPersistentStore(
    kafkaClientConfig: KafkaClientConfig,
    kafkaPersistentStoreConfig: KafkaPersistentStoreConfig,
    kafkaProducer: TransactionalProducer,
    initializedRef: Ref.Synchronized[Boolean]
) extends PersistentStore {
  import BatchMessageEnvelopeJson.given

  private object KafkaSerde {
    val key: Serde[Any, TopologyId] =
      Serde.string.inmapM(s => ZIO.succeed(TopologyId(s)))(topologyId => ZIO.succeed(topologyId.value))
    val value: Serde[Any, BatchMessageEnvelope[TopologyDeployment]] =
      Serde.string.inmapM[Any, BatchMessageEnvelope[TopologyDeployment]](s =>
        ZIO.fromEither(s.fromJson[BatchMessageEnvelope[TopologyDeployment]]).mapError(e => new RuntimeException(e))
      )(td => ZIO.succeed(td.toJson))
  }

  override def loadLatest(): Task[TopologyDeployments] =
    initializeIfNeeded *>
      ZIO.scoped {
        for {
          consumer <- Consumer.make(
            ConsumerSettings(kafkaClientConfig.brokersList, kafkaClientConfig.additionalConfig).withOffsetRetrieval(
              Consumer.OffsetRetrieval.Auto(AutoOffsetStrategy.Earliest)
            )
          )
          _ <- ZIO.logInfo(s"loading the topologies from the $topicName kafka topic")
          deserializedTopologiesWithDuration <- consumeUntilEnd(consumer, topicName).timed
          (duration, deserializedTopologies) = deserializedTopologiesWithDuration

          failedTopologies = deserializedTopologies.collect { case (id, Left(jsonError)) =>
            ZIO.logError(s"failed to deserialize topology $id: $jsonError")
          }.toList

          topologies = deserializedTopologies
            .collect { case (_, Right(topology)) => topology }
            .map(td => (td.topologyId, td))
            .toMap

          _ <- ZIO.foreachDiscard(failedTopologies)(identity)
          _ <- ZIO.logInfo(
            s"finished loading ${topologies.size} topologies, discarded ${failedTopologies.size} from the $topicName kafka topic in ${duration.toMillis / 1000.0} seconds"
          )
        } yield topologies
      }

  override def save(topologyDeployments: TopologyDeployments): Task[Unit] =
    initializeIfNeeded *> ZIO.scoped {
      for {
        txn <- kafkaProducer.createTransaction
        batchSize = topologyDeployments.size
        _ <- ZIO.foreachDiscard(topologyDeployments.values.zipWithIndex) { (td, indexInBatch) =>
          for {
            recordMetadataWithDuration <- txn
              .produce(
                topicName,
                td.topologyId,
                BatchMessageEnvelope(batchSize, indexInBatch, td),
                KafkaSerde.key,
                KafkaSerde.value,
                offset = None
              )
              .timed
            (duration, recordMetadata) = recordMetadataWithDuration
            _ <- ZIO.logInfo(
              s"saved topology ${td.topologyId} into $topicName kafka topic: P#${recordMetadata.partition} @${recordMetadata.offset} in ${duration.toMillis / 1000.0} seconds"
            )
          } yield ()
        }
      } yield ()
    }

  override def stream(compactHistory: Boolean): Stream[Throwable, TopologyDeployments] =
    ZStream.unwrapScoped {
      initializeIfNeeded *> ZIO.attempt {
        ???
      }
    }

  private def initializeIfNeeded: Task[Unit] = for {
    _ <- initializedRef.updateZIO { initialized =>
      // initialize if it hasn't been and either way set initializedRef to true
      initialize.unless(initialized) *> ZIO.succeed(true)
    }
  } yield ()

  private def initialize: Task[Unit] = {
    ZIO.scoped {
      for {
        adminClient <- AdminClient.make(kafkaClientConfig.toAdminClientSettings)
        topics <- adminClient.listTopics()
        _ <-
          if (topics.contains(topicName)) {
            if (topicConfig.reCreate) {
              recreateTopicZIO(adminClient)
            } else {
              updateTopicZIO(adminClient)
            }
          } else {
            createTopicZIO(adminClient)
          }
      } yield ()
    }
  }

  private def createTopicZIO(adminClient: AdminClient) = {
    val numPartitions = 1
    val replicationFactor = topicConfig.replicationFactor.getOrElse(-1.toShort)
    val config = kafkaTopicConfig
    val newTopic = AdminClient.NewTopic(topicName, numPartitions, replicationFactor, config)
    for {
      _ <- ZIO.logInfo(s"Creating topic $topicName...")
      _ <- adminClient.createTopic(newTopic)
      _ <- ZIO.logInfo(
        s"Topic $topicName created successfully: partitions = $numPartitions, replication-factor = $replicationFactor, config = $config"
      )
    } yield ()
  }

  private def deleteTopicZIO(adminClient: AdminClient) =
    ZIO.logInfo(s"Deleting topic $topicName...") *> adminClient.deleteTopic(topicName)

  private def recreateTopicZIO(adminClient: AdminClient) = {
    val delay = 5.seconds
    // Waiting 5 seconds so that it can be properly deleted (TODO refactor this to a common place)
    deleteTopicZIO(adminClient)
      *> ZIO.logInfo(s"Waiting for ${delay.toSeconds} seconds before creating topic $topicName...")
      *> ZIO.sleep(delay)
      *> createTopicZIO(adminClient)
  }

  private def updateTopicZIO(adminClient: AdminClient) = {
    for {
      _ <- ZIO.logInfo(s"Topic $topicName already exists")
      topicsDescriptions <- adminClient.describeTopics(Seq(topicName))
      topicDescriptions <- ZIO.getOrFailWith(RuntimeException(s"Could not describe topic $topicName"))(topicsDescriptions.get(topicName))

      numPartitions <- ZIO.getOrFailWith(RuntimeException(s"Topic $topicName has no partition infos."))(
        topicDescriptions.partitions.map(_.partition).maxOption.map(_ + 1)
      )
      _ <- ZIO
        .fail(
          RuntimeException(
            s"Topic $topicName has $numPartitions partitions but expected 1. Use a different topic for persistence or delete the $topicName topic and let kafkakewl re-create it with the right number of partitions"
          )
        )
        .unless(numPartitions == 1)

      numReplicas = topicDescriptions.partitions.head.replicas.length
      config <- adminClient.getDynamicTopicConfig(topicName)
      // TODO check replicas
      // TODO check config, alter config if possible (unless the replica constraints have changed)
      _ <- ZIO.logInfo(s"Topic $topicName partitions = $numPartitions, replicas = $numReplicas, config = $config")
    } yield ()
  }

  private def consumeUntilEnd(consumer: Consumer, topic: String): Task[Map[TopologyId, Either[String, TopologyDeployment]]] = {

    for {
      topicPartitions <- consumer.partitionsFor(topic)
      partitionMapAndRecords <- KafkaConsumerUtils.consumeUntilEnd(
        consumer,
        topicPartitions.map(tpi => new TopicPartition(tpi.topic(), tpi.partition())),
        Deserializer.string,
        Deserializer.string
      )
      (partitionMap, records) = partitionMapAndRecords

      topologyMap = records
        .foldLeft(collection.mutable.Map.empty[String, String]) {
          case (map, record) => {
            map.update(record.key(), record.value())
            map
          }
        }
        .map { case (key, value) => (TopologyId(key), value.fromJson[BatchMessageEnvelope[TopologyDeployment]].map(_.payload)) }
        .toMap

    } yield topologyMap
  }
  private def topicConfig = kafkaPersistentStoreConfig.topic
  private def topicName = topicConfig.name
  private val defaultKafkaTopicConfig = Map(TopicConfig.RETENTION_MS_CONFIG -> "-1")
  private def kafkaTopicConfig = defaultKafkaTopicConfig ++ topicConfig.config
}

object KafkaPersistentStore {
  val live: ZLayer[KafkaClientConfig & KafkaPersistentStoreConfig, Throwable, KafkaPersistentStore] =
    ZLayer.scoped {
      for {
        kafkaClientConfig <- ZIO.service[KafkaClientConfig]
        kafkaPersistentStoreConfig <- ZIO.service[KafkaPersistentStoreConfig]
        producer <- TransactionalProducer.make(
          TransactionalProducerSettings(
            kafkaClientConfig.brokersList,
            30.seconds,
            properties = kafkaClientConfig.additionalConfig,
            kafkaPersistentStoreConfig.transactionalId,
            4096
          )
        )
        initialized <- Ref.Synchronized.make(false)
      } yield KafkaPersistentStore(kafkaClientConfig, kafkaPersistentStoreConfig, producer, initialized)
    }
}
