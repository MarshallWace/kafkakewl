/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/** Some of the decoding logic originates from
  *   - https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go
  *   - https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala
  *     (converted to scala)
  */

package com.mwam.kafkakewl.metrics.services

import com.mwam.kafkakewl.common.kafka.KafkaConsumerExtensions.*
import com.mwam.kafkakewl.common.kafka.{
  CompactedConsumeResult,
  KafkaConsumerUtils
}
import com.mwam.kafkakewl.domain.config.KafkaClientConfig
import com.mwam.kafkakewl.metrics.ConsumerOffsetsSourceConfig
import com.mwam.kafkakewl.metrics.domain.{
  ConsumerGroupTopicPartition,
  KafkaConsumerGroupOffset,
  KafkaConsumerGroupOffsets
}
import com.mwam.kafkakewl.utils.*
import com.mwam.kafkakewl.utils.CollectionExtensions.*
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer as KafkaDeserializer
import zio.*
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer.{Consumer, ConsumerSettings, Subscription}
import zio.kafka.serde.*
import zio.metrics.*
import zio.stream.*

object ConsumerOffsetsDeserializers {

  import java.nio.charset.StandardCharsets
  import java.nio.{ByteBuffer, ByteOrder}
  import java.time.{Instant, ZoneOffset}
  import java.util

  implicit class ByteBufferExtensions(bb: ByteBuffer) {

    /** Reads a length-prefixed string from the byte-buffer. If the 16 bit
      * length is -1 (0xFFFF) it considers it an empty string.
      *
      * This is how the `__consumer_offsets` topic's key and value encodes
      * strings.
      */
    def getStringPrefixedWithLength: String = {
      val length = bb.getShort()
      if (length == -1) {
        ""
      } else {
        val stringBytes = Array.fill(length)(0.toByte)
        bb.get(stringBytes, 0, length)
        new String(stringBytes, StandardCharsets.UTF_8)
      }
    }
  }

  /** A kafka deserializer returning a KafkaConsumerGroupTopicPartition from the
    * `__consumer_offsets` topic's keys.
    *
    * It supports only version 0 or 1 and return null for version 2 (which means
    * consumer group metadata messages will be filtered out).
    */
  class ConsumerOffsetsKeyDeserializer
      extends KafkaDeserializer[ConsumerGroupTopicPartition] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      ()

    override def deserialize(
        topic: String,
        data: Array[Byte]
    ): ConsumerGroupTopicPartition = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
      val version = bb.getShort()
      version match {
        case 0 | 1 =>
          val consumerGroup = bb.getStringPrefixedWithLength
          val topic = bb.getStringPrefixedWithLength
          val partition = bb.getInt()
          ConsumerGroupTopicPartition(consumerGroup, topic, partition)

        case 2 =>
          // version 2 is about consumer group metadata, which we ignore and filter out completely
          null
        case _ =>
          sys.error(
            s"Unsupported key version: $version. Check the code here: https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go#L388 or here: https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1144"
          )
      }
    }

    override def close(): Unit = ()
  }

  /** A kafka deserializer returning a ConsumerGroupOffset from the
    * `__consumer_offsets` topic's values.
    *
    * It can only be used if the key's version is 0 or 1, otherwise the format
    * of this is different (the value of consumer group metadata messages).
    */
  class ConsumerOffsetsValueDeserializer
      extends KafkaDeserializer[KafkaConsumerGroupOffset] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      ()

    override def deserialize(
        topic: String,
        data: Array[Byte]
    ): KafkaConsumerGroupOffset = {
      val bb = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN)
      val version = bb.getShort()
      val (offset, metadata, timestamp) = version match {
        case 0 | 1 =>
          val offset = bb.getLong()
          val metadata = bb.getStringPrefixedWithLength
          val timestamp = bb.getLong()
          (offset, metadata, timestamp)
        case 3 =>
          val offset = bb.getLong()
          val leaderEpoch =
            bb.getInt() // this is not needed, but we still need to skip it
          val metadata = bb.getStringPrefixedWithLength
          val timestamp = bb.getLong()
          (offset, metadata, timestamp)
        case _ =>
          sys.error(
            s"Unsupported value version: $version. Check the code here: https://github.com/linkedin/Burrow/blob/07bc78809aa4e5e879d13bfe0c827faa64c85cf1/core/internal/consumer/kafka_client.go#L465 or here: https://github.com/apache/kafka/blob/master/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1163"
          )
      }
      val offsetDateTime =
        Instant.ofEpochMilli(timestamp).atOffset(ZoneOffset.UTC)
      KafkaConsumerGroupOffset(offset, metadata, offsetDateTime)
    }

    override def close(): Unit = ()
  }

  val keyDeserializer: ConsumerOffsetsKeyDeserializer =
    new ConsumerOffsetsKeyDeserializer()
  val valueDeserializer: ConsumerOffsetsValueDeserializer =
    new ConsumerOffsetsValueDeserializer()
}

class ConsumerOffsetsSource(
    private val scope: Scope, // TODO This is slightly dangerous, the scope must not be used after it's closed
    private val consumerGroupOffsetsStream: ZStream[
      Any,
      Throwable,
      KafkaConsumerGroupOffsets
    ],
    private val hub: Hub[KafkaConsumerGroupOffsets]
) {
  def subscribe(): URIO[Scope, Dequeue[KafkaConsumerGroupOffsets]] =
    hub.subscribe

  def startPublishing(): UIO[Unit] = for {
    consumerGroupOffsetsFiber <- consumerGroupOffsetsStream
      .runScoped(ZSink.foreach(hub.publish))
      .provideEnvironment(ZEnvironment(scope))
      .orDie
      .fork // TODO error handling!
    // TODO not expecting subscribers after this -> those would not get the initial snapshot of topic infos, only diffs
    _ <- scope.addFinalizer(consumerGroupOffsetsFiber.interrupt *> hub.shutdown)
  } yield ()
}

object ConsumerOffsetsSource {
  private val lagGauge = Metric.gauge("kafkakewl_metrics_consumer_offsets_lag")

  // TODO remove Throwable
  val live: ZLayer[
    KafkaClientConfig & ConsumerOffsetsSourceConfig,
    Throwable,
    ConsumerOffsetsSource
  ] =
    ZLayer.scoped {
      for {
        scope <- ZIO.service[Scope]
        kafkaClientConfig <- ZIO.service[KafkaClientConfig]
        consumerOffsetsSourceConfig <- ZIO.service[ConsumerOffsetsSourceConfig]
        hub <- Hub.bounded[KafkaConsumerGroupOffsets](requestedCapacity = 1)
      } yield ConsumerOffsetsSource(
        scope,
        // The stream is wrapped in a ZIO which we don't flatMap above because that would mean, we'll wait
        // until the initial consumption to the end of the consumer offset topic finishes.
        // Instead we unwrap the ZIO[_,_,ZStream[_,_,_]] and the stream will start emitting when it's ready.
        ZStream.unwrapScoped(
          createConsumerGroupOffsetsStream(
            kafkaClientConfig,
            consumerOffsetsSourceConfig
          )
        ),
        hub
      )
    }

  private def createConsumerGroupOffsetsStream(
      kafkaClientConfig: KafkaClientConfig,
      consumerOffsetsSourceConfig: ConsumerOffsetsSourceConfig
  ): ZIO[Scope, Throwable, ZStream[Any, Throwable, KafkaConsumerGroupOffsets]] =
    for {
      // Compacted, initial load
      durationCompactedConsumeResult <- loadLatestConsumerOffsets(
        kafkaClientConfig,
        consumerOffsetsSourceConfig
      ).timed
      (duration, ccr) = durationCompactedConsumeResult
      _ <- ZIO.logInfo(
        s"consumed ${ccr.noOfConsumedMessages} messages, ${ccr.lastValues.size} unique keys in ${duration.toMillis / 1000.0} seconds (${ccr.noOfConsumedMessages / (duration.toSeconds.toDouble)} messages/sec)"
      )
      initialConsumerGroupOffsetsStream = ZStream.succeed(
        (ccr.lastValues, ccr.nextOffsets)
      )

      // Live consumption
      // Can't get the consumer from the environment, because the ConsumerSettings contains the OffsetRetrieval where I can specify the offsets
      consumer <- Consumer.make(
        ConsumerSettings(
          kafkaClientConfig.brokersList,
          offsetRetrieval = OffsetRetrieval.Manual(topicPartitions =>
            ZIO.attempt(
              topicPartitions.view.map(tp => (tp, ccr.nextOffsets(tp))).toMap
            )
          ),
          properties = kafkaClientConfig.additionalConfig
        )
      )
      partitionInfos <- consumer.partitionsFor(
        consumerOffsetsSourceConfig.consumerOffsetsTopicName
      )
      topicPartitions = partitionInfos.map(pi =>
        new TopicPartition(pi.topic, pi.partition)
      )
      topicPartitionSet = topicPartitions.toSet

      // TODO calculate and expose the live lag as a metric
      liveConsumerGroupOffsetsStream = consumer
        .plainStream(
          Subscription.manual(
            partitionInfos.map(pi => (pi.topic, pi.partition)): _*
          ),
          Deserializer.bytes,
          Deserializer.bytes
        )
        .map { cr =>
          val topic = cr.record.topic
          val key = ConsumerOffsetsDeserializers.keyDeserializer.deserialize(
            topic,
            cr.key.get
          )
          // TODO refactor this to re-use elsewhere
          // the key deserializer can return null in which case we need to skip this key-value altogether
          if (key == null) {
            None
          } else {
            val value = Option(cr.value).map(valueBytes =>
              ConsumerOffsetsDeserializers.valueDeserializer
                .deserialize(topic, valueBytes.get)
            )
            Some((key, value))
          }
        }
        .collect { case Some(keyValue) => keyValue }
        .aggregateAsyncWithin(
          ZSink.collectAll,
          Schedule.fixed(consumerOffsetsSourceConfig.compactionInterval)
        )
        .mapZIO { chunk =>
          // compacting the chunk, by keeping the latest value per key
          val compactedConsumerGroupOffsets = chunk.toMap
          // Putting the current positions (next offsets) next to it so that below we can calculate the lag
          for {
            nextOffsets <- ZIO.foreach(topicPartitions) { tp =>
              for {
                nextOffset <- consumer.position(tp)
              } yield (tp, nextOffset)
            }
          } yield (compactedConsumerGroupOffsets, nextOffsets.toMap)
        }

      // Concatenating the 2 streams together to form a complete consumer offsets stream
      consumerGroupOffsetsStream =
        (initialConsumerGroupOffsetsStream ++ liveConsumerGroupOffsetsStream)
          .tap { case (_, nextOffsets) =>
            // the lag is sent into the gauge and discarded
            consumer
              .endOffsets(topicPartitionSet)
              .map(_.subtract(nextOffsets).values.sum.toDouble) @@ lagGauge
          }
          .map { case (consumerGroupOffsets, _) => consumerGroupOffsets }
    } yield consumerGroupOffsetsStream

  private def loadLatestConsumerOffsets(
      kafkaClientConfig: KafkaClientConfig,
      consumerOffsetsSourceConfig: ConsumerOffsetsSourceConfig
  ): RIO[Scope, CompactedConsumeResult[
    ConsumerGroupTopicPartition,
    KafkaConsumerGroupOffset
  ]] = for {
    // This consumer is used only to get the topic-partitions. For the actual consumption we'll create new ones (because we may need more than one)
    consumer <- KafkaConsumerUtils.kafkaConsumerBytesBytesZIO(kafkaClientConfig)
    topicPartitions <- ZIO.attempt(
      consumer.topicPartitionsOf(
        consumerOffsetsSourceConfig.consumerOffsetsTopicName
      )
    )
    topicPartitionSet = topicPartitions.toSet

    // The total number of offsets is exposed as lag until we finish consuming the initial snapshot (to keep things simple)
    _ <- (for {
      beginningOffsets <- ZIO.attempt(
        consumer.beginningOffsets(topicPartitionSet)
      )
      endOffsets <- ZIO.attempt(consumer.endOffsets(topicPartitionSet))
    } yield endOffsets
      .subtract(beginningOffsets)
      .values
      .sum
      .toDouble) @@ lagGauge

    // Distribute the topic-partitions
    consumeCompactZIOs = topicPartitions
      .distribute(consumerOffsetsSourceConfig.initialLoadParallelism)
      .zipWithIndex
      .map { (topicPartitions, index) =>
        ZIO
          .scoped {
            for {
              consumer <- KafkaConsumerUtils.kafkaConsumerBytesBytesZIO(
                kafkaClientConfig
              )
              compactedConsumeResult <- ZIO.attempt(
                KafkaConsumerUtils
                  .consumeCompactUntilEnd(consumer, topicPartitions)
              )
            } yield compactedConsumeResult
          }
          .timed
          .tap { case (duration, ccr) =>
            ZIO.logInfo(
              s"#$index consumed ${ccr.noOfConsumedMessages} messages, ${ccr.lastValues.size} unique keys in ${duration.toMillis / 1000.0} seconds (${ccr.noOfConsumedMessages / (duration.toSeconds.toDouble)} messages/sec)"
            )
          }
          .map { case (_, ccr) =>
            ccr.deserialize(
              consumerOffsetsSourceConfig.consumerOffsetsTopicName,
              ConsumerOffsetsDeserializers.keyDeserializer,
              ConsumerOffsetsDeserializers.valueDeserializer
            )
          }
      }
    // And consume them parallel
    // TODO long running threads
    compactedConsumeResults <- ZIO.foreachPar(consumeCompactZIOs)(identity)
  } yield compactedConsumeResults.reduce(_ + _)
}
