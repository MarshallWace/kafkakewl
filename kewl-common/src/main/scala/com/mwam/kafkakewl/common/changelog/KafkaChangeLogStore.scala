/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.changelog

import java.util.concurrent.atomic.AtomicBoolean
import com.mwam.kafkakewl.utils.{ApplicationMetrics, safely}
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaConsumerConfig, KafkaProducerConfig}
import com.mwam.kafkakewl.common.{Env, KafkaKewlSystemTopicConfig, KafkaKewlTopicUtils}
import com.mwam.kafkakewl.domain.{AllDeploymentEntitiesStateChanges, AllStateEntitiesStateChanges, ChangeLogMessage, EntityStateChangeTransactionItem}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.typesafe.scalalogging.{LazyLogging, Logger}
import io.circe.{Decoder, Encoder}
import io.circe.parser.decode
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
  * A class saving state/deployment transactions to a kafka-topic.
  *
  * @param env the environment
  * @param connection the kafka connection
  * @param systemTopicConfig the configuration for the system-topics
  * @param timeoutDuration the produce time-out
  * @param startWithWipe true if it should wipe the kafka-topic at the beginning
  */
class KafkaChangeLogStore(
  env: Env,
  connection: KafkaConnection,
  systemTopicConfig: KafkaKewlSystemTopicConfig,
  timeoutDuration: Duration,
  startWithWipe: Boolean = false
)(implicit encoder: Encoder[ChangeLogMessage], ec: ExecutionContextExecutor) extends ChangeLogStore with LazyLogging {

  private val changeLogTopic = KafkaKewlTopicUtils.changeLog

  KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(changeLogTopic), wipeAndRecreate = startWithWipe)

  private val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](KafkaConfigProperties.forProducer(KafkaProducerConfig(connection, idempotent = Some(true))))

  private def produce(
    producer: KafkaProducer[String, String],
    topic: String,
    changeLogMessage: Iterable[ChangeLogMessage]
  )(implicit encoder: Encoder[ChangeLogMessage], ec: ExecutionContextExecutor): Future[Iterable[RecordMetadata]] = {
    val produceFutures = changeLogMessage
      .map(m => producer.produce(topic, m.transactionId(), m, Some(logger)))
    Future.sequence(produceFutures)
  }

  override def saveStateChanges(transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]]): Unit = {
    producer.retryIfFails(
      p => Await.result(produce(p, changeLogTopic, transactionItems.map(t => ChangeLogMessage(state = Some(t)))), timeoutDuration),
      beforeRetry = e => {
        logger.error(s"KafkaError while producing state entity changes into the changelog", e)
        ApplicationMetrics.errorCounter.inc()
      }
    )
  }

  override def saveDeploymentStateChanges(kafkaClusterId: KafkaClusterEntityId, transactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]]): Unit = {
    producer.retryIfFails(
      p => Await.result(produce(p, changeLogTopic, transactionItems.map(t => ChangeLogMessage(deployment = Some(t), kafkaClusterId = Some(kafkaClusterId)))), timeoutDuration),
      beforeRetry = e => {
        logger.error(s"KafkaError while producing deployment entity changes into the changelog", e)
        ApplicationMetrics.errorCounter.inc()
      }
    )
  }

  def wipe(): Unit = KafkaKewlTopicUtils.createIfNeeded(env, connection, systemTopicConfig, Seq(changeLogTopic), wipeAndRecreate = true)
  def close(): Unit = producer.close()
}

final case class ReceivedChangeLogMessage(topic: String, partition: Int, offset: Long, message: ChangeLogMessage) {
  def partitionOffsetString: String = s"[P$partition-$offset]"
}

object KafkaChangeLogStore extends LazyLogging {
  def committedOffsets(consumer: KafkaConsumer[String, String], topicPartitions: Iterable[TopicPartition]): Map[TopicPartition, Long] = {
    topicPartitions
      .flatMap(tp => Option(consumer.committed(tp))
        .map(o => (tp, o.offset()))
      )
      .toMap
  }

  def consumeUntil(
    consumer: KafkaConsumer[String, String],
    untilOffsets: Map[TopicPartition, Long],
    handleMessageFunc: ReceivedChangeLogMessage => Unit,
    commitAfterMessageProcessing: Boolean
  )(implicit decoder: Decoder[ChangeLogMessage]): Unit = {

    def shouldConsume(): Boolean = {
      val endOfAllPartitions = !consumer.assignment.asScala
        .map(tp => tp -> (consumer.position(tp, java.time.Duration.ofSeconds(10)) : scala.Long))
        .exists { case (tp, o) => o < untilOffsets.getOrElse(tp, -1L) }
      !endOfAllPartitions
    }

    while (shouldConsume()) {
      val records = consumer.poll(java.time.Duration.ofMillis(1000))
      for (record <- records.asScala) {
        // TODO for now we fail-fast for decode errors, exception goes up, consumption stops. Maybe at some point we'll decide to skip these messages?
        val changeLogMessage = decode[ChangeLogMessage](record.value).right.get
        handleMessageFunc(ReceivedChangeLogMessage(record.topic(), record.partition(), record.offset(), changeLogMessage))
        if (commitAfterMessageProcessing) {
          consumer.commitSync(Map(new TopicPartition(record.topic(), record.partition()) -> new OffsetAndMetadata(record.offset() + 1)).asJava)
        }
      }
    }
  }

  def consumeUntilStopped(
    consumer: KafkaConsumer[String, String],
    stopConsuming: AtomicBoolean,
    handleMessageFunc: ReceivedChangeLogMessage => Unit,
    commitAfterMessageProcessing: Boolean
  )(implicit decoder: Decoder[ChangeLogMessage]): Unit = {
    while (!stopConsuming.get()) {
      val records = consumer.poll(java.time.Duration.ofMillis(1000))
      for (record <- records.asScala) {
        // TODO for now we fail-fast for decode errors, exception goes up, consumption stops. Maybe at some point we'll decide to skip these messages?
        val changeLogMessage = decode[ChangeLogMessage](record.value).right.get
        handleMessageFunc(ReceivedChangeLogMessage(record.topic(), record.partition(), record.offset(), changeLogMessage))
        if (commitAfterMessageProcessing) {
          consumer.commitSync(Map(new TopicPartition(record.topic(), record.partition()) -> new OffsetAndMetadata(record.offset() + 1)).asJava)
        }
      }
    }
  }

  def consumeFromBeginningWithAutoCommit(
    consumerConfig: KafkaConsumerConfig,
    handleMessageFunc: (ReceivedChangeLogMessage, Boolean) => Unit,
    reprocessCompletedFunc: () => Unit,
    stopConsuming: AtomicBoolean,
    logger: Logger
  )(implicit decoder: Decoder[ChangeLogMessage]): Unit = {
    val changeLogTopic = KafkaKewlTopicUtils.changeLog
    val consumer = new KafkaConsumer[String, String](KafkaConfigProperties.forStringConsumer(consumerConfig))
    try {
      val topicPartitions = consumer.assignPartitionsOf(Set(changeLogTopic))
      val topicPartitionCommittedOffsets = committedOffsets(consumer, topicPartitions)
      val committedOffsetsString = topicPartitionCommittedOffsets.map { case(tp, o) => s"[P${tp.partition}-$o]" }.mkString(", ")

      // starting from the beginning
      consumer.seekToBeginning(topicPartitions)

      // first we consume until the committed offset and call the message handling callback with "true", meaning "old messages"

      if (topicPartitionCommittedOffsets.nonEmpty) {
        logger.info(s"consuming $changeLogTopic from the beginning until the committed offsets $committedOffsetsString to build up state...")
        consumeUntil(consumer, topicPartitionCommittedOffsets, handleMessageFunc(_, true), commitAfterMessageProcessing = false)
        logger.info(s"consuming $changeLogTopic from the committed offsets $committedOffsetsString as new messages...")
      } else {
        logger.info(s"consuming $changeLogTopic from the beginning as new messages...")
      }

      // whether there was anything already processed or not, we're done with that
      reprocessCompletedFunc()

      // then we carry on until we need to stop, call the message handling callback with "false", meaning "new messages"
      consumeUntilStopped(consumer, stopConsuming, handleMessageFunc(_, false), commitAfterMessageProcessing = true)
    } catch safely {
      case NonFatal(e) => {
        // just logging and crashing really
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"Fatal error while consuming $changeLogTopic: $e")
        logger.error(s"A consuming thread failed, the application must be restarted.")
        throw e
      }
    } finally {
      consumer.close()
    }
  }

  def consumeFromBeginningWithoutCommit(
    consumerConfig: KafkaConsumerConfig,
    handleMessageFunc: (ReceivedChangeLogMessage, Boolean) => Unit,
    reprocessCompletedFunc: () => Unit,
    stopConsuming: AtomicBoolean,
    logger: Logger
  )(implicit decoder: Decoder[ChangeLogMessage]): Unit = {
    val changeLogTopic = KafkaKewlTopicUtils.changeLog
    val consumer = new KafkaConsumer[String, String](KafkaConfigProperties.forStringConsumer(consumerConfig))
    try {
      val topicPartitions = consumer.assignPartitionsOf(Set(changeLogTopic))
      val topicPartitionEndOffsets = consumer.endOffsets(topicPartitions)
      val endOffsetsString = topicPartitionEndOffsets.map { case(tp, o) => s"[P${tp.partition}-$o]" }.mkString(", ")

      // starting from the beginning
      consumer.seekToBeginning(topicPartitions)

      // first we consume until the end-offset and call the message handling callback with "true", meaning "old messages"

      if (endOffsetsString.nonEmpty) {
        logger.info(s"consuming $changeLogTopic from the beginning until the end offsets $endOffsetsString to build up state...")
        consumeUntil(consumer, topicPartitionEndOffsets, handleMessageFunc(_, true), commitAfterMessageProcessing = false)
        logger.info(s"consuming $changeLogTopic from the committed offsets $endOffsetsString as new messages...")
      } else {
        logger.info(s"consuming $changeLogTopic from the beginning as new messages...")
      }

      // whether there was anything already processed or not, we're done with that
      reprocessCompletedFunc()

      // then we carry on until we need to stop, call the message handling callback with "false", meaning "new messages"
      consumeUntilStopped(consumer, stopConsuming, handleMessageFunc(_, false), commitAfterMessageProcessing = false)
    } catch safely {
      case NonFatal(e) => {
        // just logging and crashing really
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"Fatal error while consuming $changeLogTopic: $e")
        logger.error(s"A consuming thread failed, the application must be restarted.")
        throw e
      }
    } finally {
      consumer.close()
    }
  }

  def consumeFromStoredWithAutoCommit(
    consumerConfig: KafkaConsumerConfig,
    handleMessageFunc: ReceivedChangeLogMessage => Unit,
    stopConsuming: AtomicBoolean,
    logger: Logger
  )(implicit decoder: Decoder[ChangeLogMessage]): Unit = {
    val changeLogTopic = KafkaKewlTopicUtils.changeLog
    val consumer = new KafkaConsumer[String, String](KafkaConfigProperties.forStringConsumer(consumerConfig))
    try {
      val topicPartitions = consumer.assignPartitionsOf(Set(changeLogTopic))
      val topicPartitionCommittedOffsets = committedOffsets(consumer, topicPartitions)
      val committedOffsetsString = topicPartitionCommittedOffsets.map { case(tp, o) => s"[P${tp.partition}-$o]" }.mkString(", ")

      // just consume from wherever we were and call the callback, no magic here
      logger.info(s"consuming $changeLogTopic from the committed offsets $committedOffsetsString as new messages...")
      consumeUntilStopped(consumer, stopConsuming, handleMessageFunc, commitAfterMessageProcessing = true)
    } catch safely {
      case NonFatal(e) => {
        // just logging and crashing really
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"Fatal error while consuming $changeLogTopic: $e")
        logger.error(s"A consuming thread failed, the application must be restarted.")
        throw e
      }
    } finally {
      consumer.close()
    }
  }
}