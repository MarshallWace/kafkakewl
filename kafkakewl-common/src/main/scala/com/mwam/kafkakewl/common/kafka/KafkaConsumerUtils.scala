/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.utils.Bytes
import zio.*
import zio.kafka.consumer.{Consumer, Subscription}
import zio.kafka.consumer.Subscription.Manual
import zio.kafka.serde.Deserializer
import zio.stream.{ZSink, ZStream}

import scala.annotation.targetName
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

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

  extension (compactedConsumeResult: CompactedConsumeResult[Bytes, Bytes]) {
    def deserialize[Key, Value <: AnyRef](
        topic: String,
        keyDeserializer: Deserializer[Any, Option[Key]],
        valueDeserializer: Deserializer[Any, Value]
    ): URIO[Any, CompactedConsumeResult[Key, Value]] = {

      for {
        lastVals <- ZIO.collectAllSuccesses(compactedConsumeResult.lastValues.map { case (keyBytes, valueBytesOption) =>
          val key = keyDeserializer
            .deserialize(topic, new RecordHeaders(), keyBytes.get())
            .some
            .orElseFail(None)
          val value = ZIO
            .fromOption(valueBytesOption)
            .flatMap(valueBytes => valueDeserializer.deserialize(topic, new RecordHeaders(), valueBytes.get()).option)
            .orElseSucceed(None)

          key.zip(value): IO[Option[Nothing], (Key, Option[Value])]
        })
      } yield (new CompactedConsumeResult[Key, Value](
        lastVals.toMap,
        compactedConsumeResult.nextOffsets,
        compactedConsumeResult.noOfConsumedMessages
      ))

    }
  }

  extension (topicPartitions: List[TopicPartition]) {
    def distribute(parallelism: Int): List[List[TopicPartition]] = {
      topicPartitions.zipWithIndex
        .map { case (tp, tpi) => (tp, tpi % parallelism) }
        .groupBy { case (_, threadIndex) => threadIndex }
        .map { case (threadIndex, tps) => (threadIndex, tps.map { case (tp, _) => tp }) }
        // the threadIndex is not important, once we sort by that, the indices will be exactly at their position in the resulting sequence
        .toList
        .sortBy { case (threadIndex, _) => threadIndex }
        .map { case (_, tps) => tps }
    }
  }

  def consumeUntilEnd[Key, Value](
      consumer: Consumer,
      topicPartitions: List[TopicPartition],
      keyDeserializer: Deserializer[Any, Key],
      valueDeserializer: Deserializer[Any, Value],
      pollTimeout: Duration = 100.millis
  ): Task[(Map[TopicPartition, Long], List[ConsumerRecord[Key, Value]])] = {
    val subscription = Manual(topicPartitions.toSet)
    for {
      endOffsets <- consumer.endOffsets(topicPartitions.toSet)
      // This will ensure that the topic stops once the messages up to the offsets
      // of endOffsets have been read. Also prevents the topic consumer going into
      // an infinite loop when there is one message.
      stopFibre <- {
        ZStream
          .repeat(())
          .schedule(Schedule.fixed(Duration.fromSeconds(1)))
          // For all topic partitions, get the current position of the consumer.
          .mapZIO(_ => ZIO.foreachPar(topicPartitions)(tp => consumer.position(tp).map(pos => (tp, pos))).map(_.toMap))
          // Check to see if we have hit the end of all topic partitions
          .takeUntil(nextOffsets => !endOffsets.exists { case (tp, endOffset) => nextOffsets.get(tp).exists(nextOffset => nextOffset < endOffset) })
          .run(ZSink.drain) *> consumer.stopConsumption
      }.fork
      partitionMapAndRecords <- consumer
        .plainStream(subscription, keyDeserializer, valueDeserializer)
        .runFold((mutable.Map.empty[TopicPartition, Long], mutable.ArrayBuffer.empty[ConsumerRecord[Key, Value]])) {
          case ((nextTopicPartitionOffsets, records), newRecord) =>
            nextTopicPartitionOffsets.update(new TopicPartition(newRecord.record.topic, newRecord.partition), newRecord.offset.offset + 1L)
            records.addOne(newRecord.record)
            (nextTopicPartitionOffsets, records)
        }
      _ <- stopFibre.join

      (partitionMap, records) = partitionMapAndRecords

    } yield (partitionMap.toMap, records.toList)
  }

  def consumeCompactUntilEnd[Key, Value](
      consumer: Consumer,
      topicPartitions: List[TopicPartition],
      keySerializer: Deserializer[Any, Key],
      valueSerializer: Deserializer[Any, Value],
      removeTombstones: Boolean = true,
      pollTimeout: Duration = 100.millis
  ): Task[CompactedConsumeResult[Key, Value]] = {

    consumeUntilEnd(
      consumer,
      topicPartitions,
      keySerializer,
      valueSerializer,
      pollTimeout
    ).map { (nextTopicPartitionOffsets, records) =>
      val lastValues = mutable.Map.empty[Key, Option[Value]]
      var noOfConsumedMessages = 0L
      for (record <- records) {
        lastValues += (record.key -> Option(record.value))
        noOfConsumedMessages += 1
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

}
