/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

import java.util
import java.time.{OffsetDateTime, Duration => JavaDuration}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import com.mwam.kafkakewl.utils._
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import cats.data.OptionT
import cats.instances.try_._
import com.typesafe.scalalogging.Logger
import io.circe.Encoder
import io.circe.syntax._
import kafka.common.KafkaException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException, SecurityDisabledException}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

class KafkaTopicCannotBeAltered(message: String) extends Exception(message) {}
class ConsumerGroupNotfound(message: String) extends Exception(message) {}
class ConsumerGroupBadState(message: String) extends Exception(message) {}

object KafkaExtensions {
  implicit class KafkaConsumerExtensions[Key, Value](kafkaConsumer: KafkaConsumer[Key, Value]) {
    def topicExists(topic: String): Boolean = kafkaConsumer.partitionsFor(topic) != null

    def partitionsOf(topics: Set[String], topicPartitionPredicate: TopicPartition => Boolean = _ => true): Iterable[TopicPartition] = {
      kafkaConsumer.listTopics().asScala
        .filterKeys(topics.contains).values
        .flatMap(_.asScala)
        .map(pi => new TopicPartition(pi.topic(), pi.partition()))
        .filter(topicPartitionPredicate)
    }

    def assignPartitionsOf(topics: Set[String], topicPartitionPredicate: TopicPartition => Boolean = _ => true): Iterable[TopicPartition] = {
      val topicPartitions = partitionsOf(topics, topicPartitionPredicate)
      kafkaConsumer.assign(topicPartitions.asJavaCollection)
      topicPartitions
    }

    def assignedNextOffsets: Map[TopicPartition, Long] = kafkaConsumer.assignment().asScala
      .map(tp => (tp, kafkaConsumer.position(tp)))
      .toMap

    def assignedBeginningOffsets: Map[TopicPartition, Long] = kafkaConsumer.beginningOffsets(kafkaConsumer.assignment()).asScala
      .map { case (tp, o) => (tp, o: Long) }
      .toMap

    def assignedEndOffsets: Map[TopicPartition, Long] = kafkaConsumer.endOffsets(kafkaConsumer.assignment()).asScala
      .map { case (tp, o) => (tp, o: Long) }
      .toMap

    def assignedTopicPartitions: Set[TopicPartition] = kafkaConsumer.assignment().asScala.toSet

    def seekToBeginning(topicPartitions: Iterable[TopicPartition]): Unit =
      kafkaConsumer.seekToBeginning(topicPartitions.asJavaCollection)

    def seekToEnd(topicPartitions: Iterable[TopicPartition]): Unit =
      kafkaConsumer.seekToEnd(topicPartitions.asJavaCollection)

    def beginningOffsets(topicPartitions: Iterable[TopicPartition]): Map[TopicPartition, Long] =
      kafkaConsumer.beginningOffsets(topicPartitions.asJavaCollection).asScala.mapValues(o => o: scala.Long).toMap

    def beginningOffsets(topicPartitions: Iterable[TopicPartition], duration: Duration): Map[TopicPartition, Long] =
      kafkaConsumer.beginningOffsets(topicPartitions.asJavaCollection, JavaDuration.ofMillis(duration.toMillis)).asScala.mapValues(o => o: scala.Long).toMap

    def endOffsets(topicPartitions: Iterable[TopicPartition]): Map[TopicPartition, Long] =
      kafkaConsumer.endOffsets(topicPartitions.asJavaCollection).asScala.mapValues(o => o: scala.Long).toMap

    def endOffsets(topicPartitions: Iterable[TopicPartition], duration: Duration): Map[TopicPartition, Long] =
      kafkaConsumer.endOffsets(topicPartitions.asJavaCollection, JavaDuration.ofMillis(duration.toMillis)).asScala.mapValues(o => o: scala.Long).toMap

    def offsetsForTimes(timeStampByTopicPartitions: Map[TopicPartition, OffsetDateTime]): Map[TopicPartition, Long] =
      kafkaConsumer.offsetsForTimes(
        timeStampByTopicPartitions.mapValues(_.toEpochSecond * 1000: java.lang.Long).asJava
      ).asScala.filter(_._2 != null).mapValues(tso => tso.offset(): scala.Long).toMap

    def offsetsForTimes(timeStampByTopicPartitions: Map[TopicPartition, OffsetDateTime], duration: Duration): Map[TopicPartition, Long] =
      kafkaConsumer.offsetsForTimes(
        timeStampByTopicPartitions.mapValues(_.toEpochSecond * 1000: java.lang.Long).asJava,
        JavaDuration.ofMillis(duration.toMillis)
      ).asScala.filter(_._2 != null).mapValues(tso => tso.offset(): scala.Long).toMap

    def commitSync(topicPartitionOffsets: Map[TopicPartition, Long]): Unit =
      kafkaConsumer.commitSync(topicPartitionOffsets.mapValues(o => new OffsetAndMetadata(o)).asJava)

    def commitSync(topicPartitionOffsets: Map[TopicPartition, Long], duration: Duration): Unit =
      kafkaConsumer.commitSync(topicPartitionOffsets.mapValues(o => new OffsetAndMetadata(o)).asJava, JavaDuration.ofMillis(duration.toMillis))

    def topicNames(): Seq[String] = kafkaConsumer.listTopics().asScala.keys.toSeq
  }

  implicit class KafkaProducerStringStringExtensions(producer: KafkaProducer[String, String])(implicit ec: ExecutionContextExecutor) {
    def produce[Key, Value : Encoder](topic: String, key: Key, value: Value, logger: Option[Logger] = None): Future[RecordMetadata] =
      produceString(topic, key.toString, if (value != null) value.asJson.noSpaces else null, logger)

    def produceString(topic: String, key: String, value: String, logger: Option[Logger] = None): Future[RecordMetadata] = {
      logger.foreach { l => l.info(s"producing into topic $topic - $key: $value") }
      val producerRecord = new ProducerRecord(topic, key, value)
      val sendFuture = producer.send(producerRecord).toScala
      sendFuture.foreach(rm => logger.foreach { l => l.info(s"finished producing into topic $topic - $key: $value (P=${rm.partition},O=${rm.offset})") })
      sendFuture
    }
  }

  implicit class KafkaProducerExtensions[Key, Value](producer: KafkaProducer[Key, Value]) {
    def retryIfFails(
      func: KafkaProducer[Key, Value] => Unit,
      beforeRetry: KafkaException => Unit = _ => (),
      shouldRetry: KafkaException => Boolean = _ => true

    ): Unit = {
      breakable {
        while (true) {
          try {
            func(producer)
            // success, breaking out of the retry-loop
            break()
          }
          catch {
            // for these the transaction doesn't need to be aborted (according to the docs) so we just rethrow and crash
            case e: ProducerFencedException => throw e
            case e: OutOfOrderSequenceException => throw e
            case e: AuthorizationException => throw e
            // transaction should be aborted and the operation should be retried
            case e: KafkaException => {
              beforeRetry(e)
              producer.abortTransaction()
              // retrying with the loop if we don't have to stop, otherwise throw up to make sure we stop
              if (!shouldRetry(e)) throw e
            }
            // in theory we don't need to abort the transaction for any other error (according to the docs) so we just let
            // other exceptions to propagate up and make it crash
          }
        }
      }
    }

    def inTransaction(
      func: KafkaProducer[Key, Value] => Unit,
      beforeRetry: KafkaException => Unit = _ => (),
      shouldRetry: KafkaException => Boolean = _ => true

    ): Unit = {
      breakable {
        while (true) {
          try {
            producer.beginTransaction()
            func(producer)
            producer.commitTransaction()
            // success, breaking out of the retry-loop
            break()
          }
          catch {
            // for these the transaction doesn't need to be aborted (according to the docs) so we just rethrow and crash
            case e: ProducerFencedException => throw e
            case e: OutOfOrderSequenceException => throw e
            case e: AuthorizationException => throw e
            // transaction should be aborted and the operation should be retried
            case e: KafkaException => {
              beforeRetry(e)
              producer.abortTransaction()
              // retrying with the loop if we don't have to stop, otherwise throw up to make sure we stop
              if (!shouldRetry(e)) throw e
            }
            // in theory we don't need to abort the transaction for any other error (according to the docs) so we just let
            // other exceptions to propagate up and make it crash
          }
        }
      }
    }
  }

  implicit class AdminClientExtensions(admin: AdminClient) {
    def topicNames(includeInternal: Boolean = false): Try[IndexedSeq[String]] = {
      for {
        topicNames <- admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names().toTry
      } yield topicNames.asScala.toIndexedSeq.sorted
    }

    def topicExists(topic: String, includeInternal: Boolean = false): Try[Boolean] = {
      for {
        topicNames <- admin.listTopics(new ListTopicsOptions().listInternal(includeInternal)).names().toTry
      } yield topicNames.contains(topic)
    }

    def describeTopicIfExists(
      topic: String,
      configSources: Set[ConfigEntry.ConfigSource] = Set(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
    ): Try[Option[(TopicDescription, Config)]] = {
      val result = for {
        topicExists <- OptionT.liftF(topicExists(topic))
        topicDescription <-
          if (topicExists) OptionT(admin.describeTopics(List(topic).asJavaCollection).all().toTry.map(_.asScala.get(topic)))
          else OptionT.none[Try, TopicDescription]
        topicConfig <-
          if (topicExists) {
            val topicConfigResource = new ConfigResource(Type.TOPIC, topic)
            val describeConfigResult = admin.describeConfigs(List(topicConfigResource).asJavaCollection).all().toTry.map(_.asScala.get(topicConfigResource))
            OptionT(
              describeConfigResult.map(_
                .map(c => new Config(
                  c.entries().asScala
                    .filter(e => configSources.contains(e.source()))
                    .asJavaCollection)
                ))
            )
          } else OptionT.none[Try, Config]
      } yield (topicDescription, topicConfig)
      result.value
    }

    def describeTopics(
      topics: Seq[String],
      configSources: Set[ConfigEntry.ConfigSource] = Set(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)
    ): Try[Map[String, (TopicDescription, Config)]] = {
      for {
        topicDescriptions <- admin.describeTopics(topics.toList.asJavaCollection).all().toTry.map(_.asScala)
        topicConfigResources = topics.map(topic => (topic, new ConfigResource(Type.TOPIC, topic))).toMap
        topicConfigs <- admin.describeConfigs(topicConfigResources.values.asJavaCollection).all().toTry.map(_.asScala)
      } yield {
        topics.flatMap(topic => {
          val topicConfigResource = topicConfigResources(topic)
          for {
            topicDescription <- topicDescriptions.get(topic)
            topicConfig <- topicConfigs.get(topicConfigResource)
          } yield (topic, (topicDescription, new Config(topicConfig.entries().asScala.filter(e => configSources.contains(e.source())).asJavaCollection)))
        }).toMap
      }
    }

    def numberOfTopicPartitions(topic: String): Try[Option[Int]] =
      describeTopicIfExists(topic).map(_.map(_._1.partitions().asScala.map(_.partition).max + 1))

    def deleteTopicIfExists(topic: String): Try[KafkaOperationResult[Unit]] = {
      for {
        topicExists <- topicExists(topic)
        result <- if (topicExists)
          admin.deleteTopics(List(topic).asJavaCollection).all().toTry.map(_ => KafkaOperationResult.tell(s"deleted: $topic"))
        else
          Success(KafkaOperationResult.tell(s"nothing: $topic does not exists, nothing to do"))
      } yield result
    }

    def createTopic(
      topic: String,
      partitions: Int = 1,
      replicationFactor: Short = 3,
      config: Map[String, String]
    ): Try[KafkaOperationResult[Unit]] = {
      val newTopic = new NewTopic(topic, partitions, replicationFactor).configs(config.asJava)
      for {
        _ <- admin.createTopics(List(newTopic).asJavaCollection).all().toTry
      } yield KafkaOperationResult.empty
    }

    def alterTopicConfig(
      topic: String,
      config: Map[String, String]
    ): Try[KafkaOperationResult[Unit]] = {
      val configs = config.map {
        case (k, v) if v.length > 0 => new ConfigEntry(k, v)
      }.asJavaCollection
      // I don't want to upgrade to the new incrementalAlterConfigs method yet, because that needs broker version 2.3.0.
      //noinspection ScalaDeprecation
      for {
        _ <- admin.alterConfigs(Map(new ConfigResource(Type.TOPIC, topic) -> new Config(configs)).asJava).all().toTry
      } yield KafkaOperationResult.empty
    }

    def createOrAlterTopic(
      topic: String,
      partitions: Int = 1,
      replicationFactor: Short = 3,
      config: Map[String, String]
    ): Try[KafkaOperationResult[Unit]] = {
      def alterTopicIfPossible(existingTopicDescription: TopicDescription, existingTopicConfig: Map[String, String]): Try[KafkaOperationResult[Unit]] = {
        if (existingTopicDescription.partitions.size != partitions) {
          Failure(new KafkaTopicCannotBeAltered(s"Existing topic $topic has different number of partitions: ${existingTopicDescription.partitions.size} != $partitions"))
        } else if (replicationFactor >= 0 && existingTopicDescription.partitions.get(0).replicas.size != replicationFactor) {
          Failure(new KafkaTopicCannotBeAltered(s"Existing topic $topic has different replication factor: ${existingTopicDescription.partitions.get(0).replicas.size} != $replicationFactor"))
        } else if (existingTopicConfig != config && (existingTopicConfig.keySet -- config.keySet).nonEmpty) {
          Failure(new KafkaTopicCannotBeAltered(s"Existing topic $topic has more config keys: ${existingTopicConfig.keySet -- config.keySet}"))
        }
        else {
          // partitions, replication factor are the same. If the config is different, we just alter, otherwise do nothing
          if (existingTopicConfig != config) {
            alterTopicConfig(topic, config).map(_ => KafkaOperationResult.tell(s"updated: $topic's config altered from $existingTopicConfig to $config"))
          } else {
            // as far as kafka is concerned this topic already exists with the exact same attributes, so nothing to do
            Success(KafkaOperationResult.tell(s"nothing: $topic already exists with the exact same attributes, nothing to do"))
          }
        }
      }

      for {
        topicDescriptionConfigOrNone <- describeTopicIfExists(topic)
        result <- topicDescriptionConfigOrNone match {
          case Some((topicDescription, topicConfig)) =>
            alterTopicIfPossible(topicDescription, topicConfig.entries().asScala.map(e => (e.name, e.value)).toMap)
          case None =>
            createTopic(topic, partitions, replicationFactor, config)
              .map(_ => KafkaOperationResult.tell(s"created: $topic, partitions=$partitions, replicationFactor=$replicationFactor, config=$config"))
        }
      } yield result
    }

    def describeAcls(
      resourceType: ResourceType = ResourceType.ANY,
      patternType: PatternType = PatternType.ANY,
      resourceName: Option[String] = None,
      ignoreSecurityDisabledException: Boolean = true
    ): Try[IndexedSeq[AclBinding]] = {
      val resourcePatternFilter = new ResourcePatternFilter(resourceType, resourceName.orNull, patternType)
      val accessControlEntryFilter = AccessControlEntryFilter.ANY
      for {
        aclBindings <- admin.describeAcls(new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter)).values().toTry
          .recover {
            case e: SecurityDisabledException if ignoreSecurityDisabledException => new util.ArrayList[AclBinding]
          }
      } yield aclBindings.asScala.toIndexedSeq
    }

    def isSecurityEnabled: Boolean = {
      admin.describeAcls(ResourceType.CLUSTER).toEither
        .map(_ => true)
        // we're getting SecurityDisabledException if there is no authorizer in a kafka cluster
        .left.map(_.isInstanceOf[SecurityDisabledException])
        .merge
    }

    def createAcl(
      resourceType: ResourceType,
      resourcePatternType: PatternType,
      resourceName: String,
      principal: String,
      host: String,
      operation: AclOperation,
      permission: AclPermissionType
    ): Try[KafkaOperationResult[Unit]] = {
      val resourcePattern = new ResourcePattern(resourceType, resourceName, resourcePatternType)
      val accessControlEntry = new AccessControlEntry(principal, host, operation, permission)
      for {
        _ <- admin.createAcls(List(new AclBinding(resourcePattern, accessControlEntry)).asJavaCollection).all().toTry
      } yield KafkaOperationResult.empty
    }

    def deleteAcl(
      resourceType: ResourceType,
      resourcePatternType: PatternType,
      resourceName: String,
      principal: String,
      host: String,
      operation: AclOperation,
      permission: AclPermissionType
    ): Try[KafkaOperationResult[Unit]] = {
      val resourcePatternFilter = new ResourcePatternFilter(resourceType, resourceName, resourcePatternType)
      val accessControlEntryFilter = new AccessControlEntryFilter(principal, host, operation, permission)
      for {
        _ <- admin.deleteAcls(List(new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter)).asJavaCollection).all().toTry
      } yield KafkaOperationResult.empty
    }

    def describeConsumerGroups(consumerGroups: Iterable[String]): Try[Map[String, ConsumerGroupDescription]] = {
      for {
        groups <- admin.describeConsumerGroups(consumerGroups.asJavaCollection).all().toTry
      } yield groups.asScala.toMap
    }

    def allConsumerGroups(): Try[Iterable[ConsumerGroupListing]] = for {
      consumerGroups <- admin.listConsumerGroups().all().toTry
    } yield consumerGroups.asScala

    def consumerGroupOffsets(consumerGroupId: String): Try[Map[TopicPartition, OffsetAndMetadata]] = for {
      topicPartitionsOffsets <- admin.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().toTry
    } yield topicPartitionsOffsets.asScala.toMap

    def consumerGroupOffsets(consumerGroupIds: Iterable[String]): (Map[String, WithUtcTimestamp[Throwable]], Map[String, WithUtcTimestamp[Map[TopicPartition, OffsetAndMetadata]]]) = {
      val resultOrErrors = consumerGroupIds
        .map(cg => (cg, WithUtcTimestamp(consumerGroupOffsets(cg))))
      val errors = resultOrErrors.collect { case (cg, WithUtcTimestamp(ts, Failure(t))) => (cg, WithUtcTimestamp(ts, t)) }.toMap
      val results = resultOrErrors.collect { case (cg, WithUtcTimestamp(ts, Success(r))) => (cg, WithUtcTimestamp(ts, r)) }.toMap
      (errors, results)
    }

    def describeConsumerGroup(consumerGroupId: String): Try[ConsumerGroupDescription] = for {
      consumerGroupDescriptions <- admin.describeConsumerGroups(Seq(consumerGroupId))
      consumerGroupDescription <- consumerGroupDescriptions
        .get(consumerGroupId)
        .map(Success(_))
        .getOrElse(Failure(new ConsumerGroupNotfound(s"couldn't find consumer group description for '$consumerGroupId'")))
    } yield consumerGroupDescription
  }

  implicit class TopicPartitionsExtensions(topicPartitions: Iterable[TopicPartition]) {
    def toPrettyString: String =
      topicPartitions
        .toSeq
        .groupBy(_.topic)
        .map { case (topic, tps) =>
          (topic,
            tps
              .map(_.partition)
              .sorted
              .map(p => s"P=$p")
              .mkString(", ")
          )
        }
        .toSeq.sortBy { case (t, _) => t }
        .map { case (t, d) => s"$t: $d"}
        .mkString("; ")
  }

  implicit class TopicPartitionOffsetsExtensions(topicPartitionOffsets: Iterable[(TopicPartition, Long)]) {
    def toPrettyString: String =
      topicPartitionOffsets
        .toSeq
        .groupBy { case (tp, _) => tp.topic }
        .map { case (topic, tpos) =>
          (topic,
            tpos
              .map { case (tp, o) => (tp.partition, o) }
              .sortBy { case (p, _) => p }
              .map { case (p, o) => s"P=$p,O=$o" }
              .mkString(", ")
          )
        }
        .toSeq.sortBy { case (t, _) => t }
        .map { case (t, d) => s"$t: $d"}
        .mkString("; ")
  }
}
