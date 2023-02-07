/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka.utils

import cats.syntax.option._
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Bytes

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.{Failure, Success, Try}

object KafkaUpsertConsumer {
  /**
   * The result of loading the latest keys and their values.
   *
   * @param latestKeyValues the latest keys and their values, including tombstones as nones.
   * @param offsets the topic-partitions' offsets after the consumed messages
   * @param noOfProcessedMessages the total number of consumed messages
   */
  final case class LatestKeyValuesWithOffsets[K, V](
    latestKeyValues: Map[K, Option[V]],
    offsets: Map[TopicPartition, Long],
    noOfProcessedMessages: Long
  ) {
    def +(other: LatestKeyValuesWithOffsets[K, V]): LatestKeyValuesWithOffsets[K, V] =
      LatestKeyValuesWithOffsets[K, V](
        latestKeyValues ++ other.latestKeyValues,
        offsets ++ other.offsets,
        noOfProcessedMessages + other.noOfProcessedMessages
      )

    def filterKeys(f: K => Boolean): LatestKeyValuesWithOffsets[K, V] = copy(latestKeyValues = latestKeyValues.filterKeys(f))

    lazy val latestKeyValuesWithoutTombstones: Map[K, V] = latestKeyValues.collect { case (k, Some(v)) => (k, v) }
    lazy val latestKeysWithTombstones: Set[K] = latestKeyValues.collect { case (k, None) => k }.toSet
  }

  object LatestKeyValuesWithOffsets {
    def empty[K, V]: LatestKeyValuesWithOffsets[K, V] = LatestKeyValuesWithOffsets[K, V](Map.empty, Map.empty, noOfProcessedMessages = 0L)
  }

  def createBytesConsumer(
    kafkaConnection: KafkaConnection,
    groupId: Option[String] = None
  ): KafkaConsumer[Bytes, Bytes] = {
    new KafkaConsumer[Bytes, Bytes](KafkaConfigProperties.forBytesConsumer(KafkaConsumerConfig(kafkaConnection, groupId, enableAutoCommit = groupId.nonEmpty)))
  }

  def getTopicPartitions(
    kafkaConnection: KafkaConnection,
    topic: String
  ): Seq[TopicPartition] = {
    Using(createBytesConsumer(kafkaConnection)) { consumer =>
      consumer.partitionsFor(topic).asScala
        .map(p => new TopicPartition(p.topic(), p.partition()))
        .toList
    }
  }

  /**
   * Distributes the specified topic-partitions across the number of threads evenly.
   *
   * @param topicPartitions the topic-partitions to distribute
   * @param noOfThreads the number of consumer threads.
   * @return a sequence of noOfThreads elements, each with a sequence of topic-partitions that that thread needs to process.
   */
  def distributePartitionsToConsumerThreads(topicPartitions: Seq[TopicPartition], noOfThreads: Int): Seq[Seq[TopicPartition]] = {
    topicPartitions
      .zipWithIndex
      .map { case (tp, tpi) => (tp, tpi % noOfThreads) }
      .groupBy { case (_, threadIndex) => threadIndex }
      .map { case (threadIndex, tps) => (threadIndex, tps.map { case (tp, _) => tp }) }
      // the threadIndex is not important, once we sort by that, the indices will be exactly at their position in the resulting sequence
      .toSeq.sortBy { case (threadIndex, _) => threadIndex }
      .map { case (_, tps) => tps }
  }


  /**
   * Loads the latest keys and their values from kafka upsert topic-partitions.
   *
   * @param kafkaConnection the kafka-connection to use
   * @param topicPartitions the kafka topic-partitions to load
   * @param noOfThreads the number of threads to use
   * @param logger the optional logger
   * @param clientIdPrefix the prefix for the client.id config
   * @param consumerGroup the optional consumer group to use
   * @param pollTimeout the kafka consumer's poll timeout, defaults to 500 milliseconds
   * @param ec the execution context to start up consumer threads
   * @tparam K the key type
   * @tparam V the value type
   * @return the latest keys and their values with the next topic-partition offsets.
   */
  def loadLatestKeyValuesOfTopicPartitions[K: Deserializer, V: Deserializer](
    kafkaConnection: KafkaConnection,
    topicPartitions: Seq[TopicPartition],
    noOfThreads: Int,
    logger: Option[Logger] = None,
    clientIdPrefix: String = "",
    consumerGroup: Option[String] = None,
    pollTimeout: FiniteDuration = 500.millis
  )(implicit ec: ExecutionContext): Future[LatestKeyValuesWithOffsets[K, V]] = {
    val noOfPartitions = topicPartitions.size
    require(noOfThreads > 0, s"the number of threads ($noOfThreads) must be greater than 0")

    val actualNoOfThreads = Math.min(noOfPartitions, noOfThreads)

    val topicPartitionsOfThreads = distributePartitionsToConsumerThreads(topicPartitions, actualNoOfThreads)
    for (threadIndex <- 0 until actualNoOfThreads)
      logger.foreach(_.info(s"consumer#$threadIndex: consuming ${topicPartitionsOfThreads(threadIndex).toPrettyString}"))

    val beforeNanos = System.nanoTime()
    val consumerThreads = (0 until actualNoOfThreads)
      .map { threadIndex =>
        val topicPartitions = topicPartitionsOfThreads(threadIndex)
        Future {
          blocking {
            consumeCompactTopicPartitionsUntilTheEnd[K, V](
              clientIdPrefix,
              threadIndex,
              kafkaConnection,
              topicPartitions,
              logger,
              consumerGroup,
              pollTimeout
            )
          }
        }
      }

    Future.sequence(consumerThreads)
      .map { results => results.reduce(_ + _) }
      .map { result =>
        val durationSeconds = durationSince(beforeNanos).toSecondsDouble
        logger.foreach(_.info(f"consumer#all: processed ${result.noOfProcessedMessages} messages in $durationSeconds%.3f seconds (${result.noOfProcessedMessages / durationSeconds}%.3f messages/second)"))
        result
      }
  }

  /**
   * Loads the latest keys and their values from a kafka upsert topic.
   *
   * @param kafkaConnection the kafka-connection to use
   * @param topic the kafka topic to load
   * @param noOfThreads the number of threads to use
   * @param logger the optional logger to use
   * @param clientIdPrefix the prefix for the client.id config
   * @param consumerGroup the optional consumer group to use
   * @param pollTimeout the kafka consumer's poll timeout, defaults to 500 milliseconds
   * @param ec the execution context to start up consumer threads
   * @tparam K the key type
   * @tparam V the value type
   * @return the latest keys and their values with the next topic-partition offsets.
   */
  def loadLatestKeyValuesOfTopic[K: Deserializer, V: Deserializer](
    kafkaConnection: KafkaConnection,
    topic: String,
    noOfThreads: Int,
    logger: Option[Logger] = None,
    clientIdPrefix: String = "",
    consumerGroup: Option[String] = None,
    pollTimeout: FiniteDuration = 500.millis,
  )(implicit ec: ExecutionContext): Future[LatestKeyValuesWithOffsets[K, V]] = {
    loadLatestKeyValuesOfTopicPartitions[K, V](
      kafkaConnection,
      getTopicPartitions(kafkaConnection, topic),
      noOfThreads,
      logger,
      clientIdPrefix,
      consumerGroup,
      pollTimeout,
    )
  }

  /**
   * Consumes the specified topic-partitions with compacting messages every compactionInterval.
   *
   * @param kafkaConnection the kafka-connection to use
   * @param startOffsets the start offsets to start consumption from
   * @param handleKeyValuesWithLag the callback to handle a batch of compacted messages (and the current lag)
   * @param handleEndOffsetsFailure the callback to handle a failed end-offsets retrieval for the given topic-partitions
   * @param handleConsumerError the callback to handle any error that comes out of the consumer threads
   * @param shouldStop an atomic boolean indicating when the consumption should stop
   * @param noOfThreads the number of threads to use
   * @param compactionInterval the interval at which it should compact the received messages
   * @param logger the optional logger to use
   * @param clientIdPrefix the prefix for the client.id config
   * @param consumerGroup the optional consumer group to use
   * @param pollTimeout the kafka consumer's poll timeout, defaults to 500 milliseconds
   * @param ec the execution context to start up consumer threads
   * @tparam K the key type
   * @tparam V the value type
   * @return nothing
   */
  def consumeTopicPartitions[K: Deserializer, V: Deserializer](
    kafkaConnection: KafkaConnection,
    startOffsets: Map[TopicPartition, Long],
    handleKeyValuesWithLag: (LatestKeyValuesWithOffsets[K, V], Map[TopicPartition, Option[Long]]) => Unit,
    handleEndOffsetsFailure: (Iterable[TopicPartition], Throwable) => Unit,
    handleConsumerError: Throwable => Unit,
    shouldStop: AtomicBoolean,
    noOfThreads: Int,
    compactionInterval: FiniteDuration = 1.second,
    logger: Option[Logger] = None,
    clientIdPrefix: String = "",
    consumerGroup: Option[String] = None,
    pollTimeout: FiniteDuration = 500.millis
  )(implicit ec: ExecutionContext): Future[Unit] = {
    val topicPartitions = startOffsets.keys.toSeq
    val noOfPartitions = topicPartitions.size
    require(noOfThreads > 0, s"the number of threads ($noOfThreads) must be greater than 0")

    val actualNoOfThreads = Math.min(noOfPartitions, noOfThreads)

    val topicPartitionsOfThreads = distributePartitionsToConsumerThreads(topicPartitions, actualNoOfThreads)
    for (threadIndex <- 0 until actualNoOfThreads)
      logger.foreach(_.info(s"consumer#$threadIndex: consuming ${topicPartitionsOfThreads(threadIndex).toPrettyString}"))

    Using(createBytesConsumer(kafkaConnection)) { consumerForEndOffsetsOnly =>
      // assign topic-partitions to the consumerForEndOffsetsOnly only so that we can use the assignedEndOffsets() method
      consumerForEndOffsetsOnly.assign(topicPartitions.asJava)
      val keyValuesQueue = new LinkedBlockingQueue[LatestKeyValuesWithOffsets[K, V]](100000)
      val currentEndOffsets = mutable.Map.empty[TopicPartition, Long]

      val consumerThreads = (0 until actualNoOfThreads)
        .map { threadIndex =>
          val topicPartitions = topicPartitionsOfThreads(threadIndex).toSet
          Future {
            blocking {
              consumeCompactTopicPartitions[K, V](
                clientIdPrefix,
                threadIndex,
                kafkaConnection,
                startOffsets.filterKeys(topicPartitions.contains),
                (_, latestKeyValuesWithOffsets) => keyValuesQueue.put(latestKeyValuesWithOffsets), // this will block if the queue is full which is fine, this is how we backpressure the consumer if needed
                shouldStop,
                logger,
                consumerGroup,
                pollTimeout
              )
            }
          }
        }

      // handling any error from the consumer threads
      val consumerThreadsFuture = Future.sequence(consumerThreads).map(_ => ())
      consumerThreadsFuture.onComplete {
        case Failure(t) => handleConsumerError(t)
        case Success(_) =>
      }

      // while the consumer threads are busy polling kafka, deserializing records and pushing everything into the bounded queue above,
      // here we poll the bounded queue, compact the received key-value-batches and if we haven't emitted for too long, we emit the compacted batch
      val lastProcessedOffsets = mutable.Map.empty[TopicPartition, Long]
      var compactedLatestKeyValues = LatestKeyValuesWithOffsets.empty[K, V]
      var lastEmitNanos = System.nanoTime()
      while (!shouldStop.get()) {
        // retrieve key-values from the queue and compact it into the current compacted batch
        val latestKeyValuesOrNone = Option(keyValuesQueue.poll(10, TimeUnit.MILLISECONDS))
        latestKeyValuesOrNone.foreach { latestKeyValues => compactedLatestKeyValues += latestKeyValues }

        // check if we need to emit the compacted batch or not
        val durationSinceLastEmit = durationSince(lastEmitNanos)
        if (durationSinceLastEmit.gteq(compactionInterval)) {
          // getting the end-offsets first (ignoring any error) in order to calculate the lag
          Try(consumerForEndOffsetsOnly.assignedEndOffsets) match {
            case Success(endOffsets) => currentEndOffsets ++= endOffsets
            case Failure(t) => handleEndOffsetsFailure(topicPartitions, t) // we'll just use the previous end-offsets, not much else we can do
          }

          // updating the last processed offsets
          lastProcessedOffsets ++= compactedLatestKeyValues.offsets

          // calculating the lag, keeping missing end-offsets so that we indicate the topic-partitions for which we don't know the lag
          val lags = lastProcessedOffsets
            .map { case (tp, o) =>
              val lag = currentEndOffsets.get(tp) match {
                case Some(eo) => math.max(0L, eo - o).some // make the lag zero if it's negative (can be if our end-offset is not up-to-date)
                case None => none // none indicates that we couldn't calculate the lag
              }
              (tp, lag)
            }
            .toMap

          // finally invoking the callback with the compacted message key-values and their lag
          handleKeyValuesWithLag(compactedLatestKeyValues, lags)
          compactedLatestKeyValues = LatestKeyValuesWithOffsets.empty[K, V]
          lastEmitNanos = System.nanoTime()
        }
      }

      consumerThreadsFuture
    }
  }

  private def consumeCompactTopicPartitionsUntilTheEnd[K: Deserializer, V: Deserializer](
    clientIdPrefix: String,
    threadIndex: Int,
    kafkaConnection: KafkaConnection,
    topicPartitions: Seq[TopicPartition],
    logger: Option[Logger] = None,
    consumerGroup: Option[String] = None,
    pollTimeout: FiniteDuration = 500.millis,
  ): LatestKeyValuesWithOffsets[K, V] = {
    val topics = topicPartitions.groupBy(_.topic).map { case (topic, _) => topic }.toSet
    require(topics.size == 1, s"the topicPartitions must belong to the same topic but they use multiple topics: ${topics.mkString(", ")}")
    val topic = topics.head

    val keyDeserializer = implicitly[Deserializer[K]]
    val valueDeserializer = implicitly[Deserializer[V]]

    Using(createBytesConsumer(kafkaConnection.withConfig(ConsumerConfig.CLIENT_ID_CONFIG, s"$clientIdPrefix$threadIndex"), consumerGroup)) { consumer =>
      val pollTimeoutJava = java.time.Duration.ofNanos(pollTimeout.toNanos)
      val topicPartitionsJava = topicPartitions.asJava
      consumer.assign(topicPartitionsJava)
      consumer.seekToBeginning(topicPartitionsJava)
      val endOffsets = consumer.assignedEndOffsets

      logger.foreach(_.info(f"consumer#$threadIndex: starting consuming until ${endOffsets.asInstanceOf[Iterable[(TopicPartition, Long)]].toPrettyString}"))

      val beforeNanos = System.nanoTime()
      var noOfProcessedMessages = 0L
      val mutableLastKeyValues = mutable.Map.empty[Bytes, Bytes]
      val mutableNextTopicPartitionOffsets = mutable.Map.empty[TopicPartition, Long] ++ consumer.assignedNextOffsets

      def reachedEndOfAllPartitions(): Boolean = {
        // assignedNextOffsets returns the offsets after any rolled back transaction / commit marker
        val currentAssignedNextOffsets = consumer.assignedNextOffsets
        endOffsets
          .filterKeys(currentAssignedNextOffsets.contains)
          .forall { case (tp, endOffset) => currentAssignedNextOffsets.get(tp).exists(nextOffset => nextOffset >= endOffset) }
      }

      var shouldConsume = true
      while (shouldConsume) {
        val consumerRecords = consumer.poll(pollTimeoutJava)
        shouldConsume = !reachedEndOfAllPartitions()
        consumerRecords.forEach { cr =>
          val key = cr.key()
          val value = cr.value()
          if (value == null) {
            // not keeping tombstones
            mutableLastKeyValues -= key
          } else {
            mutableLastKeyValues += (key -> value)
          }
          noOfProcessedMessages += 1
          mutableNextTopicPartitionOffsets.update(new TopicPartition(cr.topic, cr.partition), cr.offset + 1L)
        }
      }

      val latestKeyValues = mutableLastKeyValues.toMap
      val nextTopicPartitionOffsets = mutableNextTopicPartitionOffsets.toMap

      val durationSeconds = durationSince(beforeNanos).toSecondsDouble
      logger.foreach(_.info(f"consumer#$threadIndex: processed $noOfProcessedMessages messages in $durationSeconds%.3f seconds (${noOfProcessedMessages / durationSeconds}%.3f messages/second) - next offsets: ${nextTopicPartitionOffsets.asInstanceOf[Iterable[(TopicPartition, Long)]].toPrettyString}"))

      LatestKeyValuesWithOffsets(
        // deserializing here (to avoid deserialization cost for every value except the last ones)
        latestKeyValues
          .par
          .flatMap { case (k, v) =>
            val keyOrNone = Option(keyDeserializer.deserialize(topic, k.get()))
            // if the deserializer returns null for the key, we filter the key-value pair out
            keyOrNone match {
              case Some(key) =>
                // we assume that we don't have any tombstones at this point (otherwise v could be null)
                (key, Option(valueDeserializer.deserialize(topic, v.get()))).some // the value deserializer can also decide that this value is a tombstone by returning null
              case None =>
                None
            }
          }
          .seq,
        nextTopicPartitionOffsets,
        noOfProcessedMessages
      )
    }
  }

  private def consumeCompactTopicPartitions[K: Deserializer, V: Deserializer](
    clientIdPrefix: String,
    threadIndex: Int,
    kafkaConnection: KafkaConnection,
    startOffsets: Map[TopicPartition, Long],
    handleKeyValues: (Int, LatestKeyValuesWithOffsets[K, V]) => Unit,
    shouldStop: AtomicBoolean,
    logger: Option[Logger] = None,
    consumerGroup: Option[String] = None,
    pollTimeout: FiniteDuration = 500.millis,
  ): Unit = {
    val topics = startOffsets.keys.groupBy(_.topic).map { case (topic, _) => topic }.toSet
    require(topics.size == 1, s"the startOffsets must belong to the same topic but they use multiple topics: ${topics.mkString(", ")}")
    val topic = topics.head

    val keyDeserializer = implicitly[Deserializer[K]]
    val valueDeserializer = implicitly[Deserializer[V]]

    Using(createBytesConsumer(kafkaConnection.withConfig(ConsumerConfig.CLIENT_ID_CONFIG, s"$clientIdPrefix$threadIndex"), consumerGroup)) { consumer =>
      val pollTimeoutJava = java.time.Duration.ofNanos(pollTimeout.toNanos)
      val topicPartitionsJava = startOffsets.keys.toSeq.asJava
      consumer.assign(topicPartitionsJava)
      startOffsets.foreach { case (tp, o) => consumer.seek(tp, o) }

      while (!shouldStop.get()) {
        val consumerRecords = consumer.poll(pollTimeoutJava)
        if (!consumerRecords.isEmpty) {
          val mutableLastKeyValues = mutable.Map.empty[K, Option[V]]
          val mutableNextTopicPartitionOffsets = mutable.Map.empty[TopicPartition, Long] ++ consumer.assignedNextOffsets
          consumerRecords.forEach { cr =>
            // we deserialize here just so that we can avoid value deserialization if the key needs to be filtered out (e.g. for __consumer_offsets it's necessary)
            val keyOrNone = Option(keyDeserializer.deserialize(topic, cr.key.get()))
            // if the deserializer returns null for the key, we filter the key-value pair out
            keyOrNone match {
              case Some(key) =>
                val value = Option(cr.value())
                  .flatMap(v => Option(valueDeserializer.deserialize(topic, v.get())))  // the value deserializer can also decide that this value is a tombstone by returning null
                // keeping tombstones
                mutableLastKeyValues += (key -> value)
              case None => ()
            }

            mutableNextTopicPartitionOffsets.update(new TopicPartition(cr.topic, cr.partition), cr.offset + 1L)
          }

          handleKeyValues(
            threadIndex,
            LatestKeyValuesWithOffsets(mutableLastKeyValues.toMap, mutableNextTopicPartitionOffsets.toMap, consumerRecords.count)
          )
        }
      }
    }
  }
}
