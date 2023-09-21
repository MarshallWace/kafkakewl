/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{BytesDeserializer, Deserializer, StringDeserializer}
import org.apache.kafka.common.utils.Bytes
import zio.*

import scala.annotation.targetName
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object KafkaConsumerExtensions {
  extension [Key, Value](consumer: Consumer[Key, Value]) {
    def beginningOffsets(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
      consumer.beginningOffsets(topicPartitions.asJava).asScala
        .map { case (tp, o) => (tp, o: Long) }
        .toMap
    }

    def endOffsets(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
      consumer.endOffsets(topicPartitions.asJava).asScala
        .map { case (tp, o) => (tp, o: Long) }
        .toMap
    }

    def positions(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, Long] =
      topicPartitions
        .map(tp => (tp, consumer.position(tp)))
        .toMap

    def topicPartitionsOf(topic: String): List[TopicPartition] =
      consumer.partitionsFor(topic).asScala.toList
        .map(pi => new TopicPartition(pi.topic, pi.partition))
  }

  extension (topicPartitions: List[TopicPartition]) {
    def distribute(parallelism: Int): List[List[TopicPartition]] = {
      topicPartitions
        .zipWithIndex
        .map { case (tp, tpi) => (tp, tpi % parallelism) }
        .groupBy { case (_, threadIndex) => threadIndex }
        .map { case (threadIndex, tps) => (threadIndex, tps.map { case (tp, _) => tp }) }
        // the threadIndex is not important, once we sort by that, the indices will be exactly at their position in the resulting sequence
        .toList.sortBy { case (threadIndex, _) => threadIndex }
        .map { case (_, tps) => tps }
    }
  }

  extension (compactedConsumeResult: CompactedConsumeResult[Bytes, Bytes]) {
    def deserialize[Key, Value <: AnyRef](topic: String, keyDeserializer: Deserializer[Key], valueDeserializer: Deserializer[Value]): CompactedConsumeResult[Key, Value] = {
      new CompactedConsumeResult[Key, Value](
        compactedConsumeResult.lastValues
          .flatMap { case (keyBytes, valueBytesOption) =>
            val key = keyDeserializer.deserialize(topic, keyBytes.get)
            // TODO refactor this to re-use elsewhere
            // the key deserializer can return null in which case we need to skip this key-value altogether
            if (key == null) {
              None
            } else {
              val value = valueBytesOption.map(valueBytes => valueDeserializer.deserialize(topic, valueBytes.get))
              Some((key, value))
            }
          },
        compactedConsumeResult.nextOffsets,
        compactedConsumeResult.noOfConsumedMessages
      )
    }
  }
}

final case class CompactedConsumeResult[Key, Value](
  lastValues: Map[Key, Option[Value]],
  nextOffsets: Map[TopicPartition, Long],
  noOfConsumedMessages: Long
) {
  @targetName("add")
  def +(other: CompactedConsumeResult[Key, Value]): CompactedConsumeResult[Key, Value] = CompactedConsumeResult(
    lastValues ++ other.lastValues,
    nextOffsets ++ other.nextOffsets,
    noOfConsumedMessages + other.noOfConsumedMessages
  )
}

object KafkaConsumerUtils {
  import KafkaConsumerExtensions.*
  import KafkaClientConfigExtensions.*

  def kafkaConsumer[Key, Value](
    kafkaClientConfig: KafkaClientConfig,
    keyDeserializer: Deserializer[Key],
    valueDeserializer: Deserializer[Value]
  ): RIO[Scope, Consumer[Key, Value]] = {
    ZIO.fromAutoCloseable(
      ZIO.attempt(new KafkaConsumer[Key, Value](
        kafkaClientConfig.toProperties,
        keyDeserializer,
        valueDeserializer
      ))
    )
  }

  def kafkaConsumerStringStringZIO(kafkaClientConfig: KafkaClientConfig): RIO[Scope, Consumer[String, String]] =
    kafkaConsumer(kafkaClientConfig, new StringDeserializer(), new StringDeserializer())

  def kafkaConsumerBytesBytesZIO(kafkaClientConfig: KafkaClientConfig): RIO[Scope, Consumer[Bytes, Bytes]] =
    kafkaConsumer(kafkaClientConfig, new BytesDeserializer(), new BytesDeserializer())

  def consumeUntilEnd[Key, Value](
    consumer: Consumer[Key, Value],
    topicPartitions: List[TopicPartition],
    pollTimeout: Duration = 100.millis
  )(
    handleConsumerRecords: ConsumerRecords[Key, Value] => Unit
  ): Map[TopicPartition, Long] = {

      val pollTimeoutJava = java.time.Duration.ofNanos(pollTimeout.toNanos)
      val topicPartitionsJava = topicPartitions.asJava

      consumer.assign(topicPartitionsJava)
      consumer.seekToBeginning(topicPartitionsJava)
      val endOffsets = consumer.endOffsets(topicPartitions.toSet)

      var shouldConsume = true
      val nextTopicPartitionOffsets = mutable.Map.empty[TopicPartition, Long] ++ consumer.positions(topicPartitions)
      while (shouldConsume) {
        val consumerRecords = consumer.poll(pollTimeoutJava)
        val nextOffsets = consumer.positions(topicPartitions)
        shouldConsume = endOffsets.exists { case (tp, endOffset) => nextOffsets.get(tp).exists(nextOffset => nextOffset < endOffset) }
        handleConsumerRecords(consumerRecords)
        consumerRecords.forEach { cr =>
          nextTopicPartitionOffsets.update(new TopicPartition(cr.topic, cr.partition), cr.offset + 1L)
        }
      }

      nextTopicPartitionOffsets.toMap
    }

  def consumeCompactUntilEnd[Key, Value](
    consumer: Consumer[Key, Value],
    topicPartitions: List[TopicPartition],
    removeTombstones: Boolean = true,
    pollTimeout: Duration = 100.millis
  ): CompactedConsumeResult[Key, Value] = {
    val lastValues = mutable.Map.empty[Key, Option[Value]]
    var noOfConsumedMessages = 0L
    val nextTopicPartitionOffsets = consumeUntilEnd(
      consumer,
      topicPartitions,
      pollTimeout
    ) { crs =>
      crs.forEach {
        cr => {
          lastValues += (cr.key -> Option(cr.value))
          noOfConsumedMessages += 1
        }
      }
    }

    CompactedConsumeResult(
      if (removeTombstones) {
        lastValues.view.collect { case (key, Some(value)) => (key, Some(value)) }.toMap
      } else {
        lastValues.toMap
      },
      nextTopicPartitionOffsets,
      noOfConsumedMessages
    )
  }
}
