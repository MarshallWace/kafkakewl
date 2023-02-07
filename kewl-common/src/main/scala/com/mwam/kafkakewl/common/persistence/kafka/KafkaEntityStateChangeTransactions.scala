/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.persistence.kafka

import cats.Monoid
import com.mwam.kafkakewl.common.Env
import com.mwam.kafkakewl.domain.EntityStateChangeTransactionItem
import com.mwam.kafkakewl.kafka.utils.KafkaExtensions._
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConsumerConfig}
import com.mwam.kafkakewl.utils.{ApplicationMetrics, safely}
import com.typesafe.scalalogging.LazyLogging
import io.circe.parser.decode
import io.circe.{Decoder, Encoder}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata}

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.control.NonFatal

object KafkaEntityStateChangeTransactions extends LazyLogging {
  def consume[SC](
    env: Env,
    consumerConfig: KafkaConsumerConfig,
    topic: String,
    process: SC => Unit,
    stopConsuming: Option[AtomicBoolean] = None,
    untilEndOfPartitions: Boolean = true
  )(implicit stateChangesMonoid: Monoid[SC], decoder: Decoder[EntityStateChangeTransactionItem[SC]]): Unit = {
    val consumer = new KafkaConsumer[String, String](KafkaConfigProperties.forStringConsumer(consumerConfig))
    try {
      val topicPartitions = consumer.assignPartitionsOf(Set(topic))
      val endOffsets = consumer.endOffsets(topicPartitions.asJavaCollection).asScala.mapValues(x => x : scala.Long)
      consumer.seekToBeginning(topicPartitions.asJavaCollection)

      def shouldConsume() = {
        val endOfPartitions = untilEndOfPartitions && !consumer.assignment.asScala
          .map(tp => tp -> (consumer.position(tp, Duration.ofSeconds(10)) : scala.Long))
          .exists { case (tp, o) => o < endOffsets.getOrElse(tp, -1L) }
        // consume until either we are stopped or reached the end of the partitions
        !endOfPartitions && !stopConsuming.exists(_.get)
      }

      // mutating this stateChangesInTransaction instance
      var stateChangesInTransaction = stateChangesMonoid.empty

      while (shouldConsume()) {
        val records = consumer.poll(Duration.ofMillis(1000))
        for (record <- records.asScala) {
          // TODO for now we fail-fast for decode errors, exception goes up, consumption stops. Maybe at some point we'll decide to skip these messages?
          val transactionItem = decode[EntityStateChangeTransactionItem[SC]](record.value).right.get
          stateChangesInTransaction = stateChangesMonoid.combine(stateChangesInTransaction, transactionItem.stateChanges)
          if (transactionItem.lastInTransaction) {
            // the transaction has finished...
            process(stateChangesInTransaction)
            // ...empty it, for the next one
            stateChangesInTransaction = stateChangesMonoid.empty
          }
        }
      }
    } catch safely {
      case NonFatal(e) => {
        // just logging and crashing really
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"Fatal error while consuming $topic: $e")
        logger.error(s"A consuming thread failed, the application must be restarted.")
        throw e
      }
    } finally {
      consumer.close()
    }
  }

  def produce[SC](
    env: Env,
    producer: KafkaProducer[String, String],
    topic: String,
    changes: IndexedSeq[EntityStateChangeTransactionItem[SC]]
  )(implicit encoder: Encoder[EntityStateChangeTransactionItem[SC]], ec: ExecutionContextExecutor): Future[IndexedSeq[RecordMetadata]] = {
    val produceFutures = changes
      .map(t => producer.produce(topic, t.transactionId, t, Some(logger)))
    Future.sequence(produceFutures)
  }
}
