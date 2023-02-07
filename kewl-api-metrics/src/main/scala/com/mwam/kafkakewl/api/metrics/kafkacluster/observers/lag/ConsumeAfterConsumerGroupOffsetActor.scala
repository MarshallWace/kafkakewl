/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import akka.actor.{Actor, Props}
import cats.syntax.option._
import com.mwam.kafkakewl.common.ActorPreRestartLog
import com.mwam.kafkakewl.domain.Mdc
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.kafka.utils.{KafkaConfigProperties, KafkaConnection, KafkaConsumerConfig}
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Bytes

import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object ConsumeAfterConsumerGroupOffsetActor {
  def props(
    kafkaClusterId: KafkaClusterEntityId,
    pollDuration: java.time.Duration
  ): Props = Props(
    new ConsumeAfterConsumerGroupOffsetActor(kafkaClusterId, pollDuration)
  )

  def name(kafkaClusterId: KafkaClusterEntityId) = s"ConsumeAfterConsumerGroupOffsetActor-$kafkaClusterId-${UUID.randomUUID().toString.replace("-", "")}"

  private[lag] final case class SetKafkaConnection(kafkaConnection: KafkaConnection)
  private[lag] final case class ConsumeAfterConsumerGroupOffset(topicPartition: TopicPartition, consumerGroup: String, consumerGroupOffset: Long, endOffset: Long)

  private[lag] sealed trait ConsumeAfterConsumerGroupOffsetResult {
    val request: ConsumeAfterConsumerGroupOffset
  }
  private[lag] object ConsumeAfterConsumerGroupOffsetResult {
    final case class Unknown(request: ConsumeAfterConsumerGroupOffset) extends ConsumeAfterConsumerGroupOffsetResult
    final case class NoMessagesBetweenOffsets(request: ConsumeAfterConsumerGroupOffset, from: Long, to: Long, messageAtTo: Boolean) extends ConsumeAfterConsumerGroupOffsetResult {
      override def toString: String = s"NoMessagesBetweenOffsets[$from; $to; ${if (messageAtTo) "ends-with-message" else "no-message"}] = ${to - from}"
    }
  }
}

class ConsumeAfterConsumerGroupOffsetActor(
  kafkaClusterId: KafkaClusterEntityId,
  pollDuration: java.time.Duration
) extends Actor
  with ActorPreRestartLog
  with DefaultInstrumented
  with LazyLogging
  with MdcUtils
{
  import ConsumeAfterConsumerGroupOffsetActor._

  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.metrics.source.consumergroup")

  override val actorMdc: Map[String, String] = Mdc.fromKafkaClusterId(kafkaClusterId)

  var kafkaConnection: Option[KafkaConnection] = none
  var kafkaConsumer: Option[KafkaConsumer[_, _]] = none

  override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    withMDC(actorMdc) {
      super.aroundReceive(receive, msg)
    }
  }

  private def consumeAfterConsumerGroupOffset(
    consumer: KafkaConsumer[_, _],
    request: ConsumeAfterConsumerGroupOffset,
    pollDuration: java.time.Duration
  ): (ConsumeAfterConsumerGroupOffsetResult, Long, Long) = {
    val topicPartitions = Seq(request.topicPartition).asJava
    consumer.assign(topicPartitions)
    consumer.seek(request.topicPartition, request.consumerGroupOffset)
    var stop = false
    var nextMessageOffset: Option[Long] = none
    var lastOffset: Option[Long] = none
    var positionCheckCount = 0L
    var positionTotalDurationMillis = 0L
    while (!stop) {
      val records = consumer.poll(pollDuration)
      records.asScala.headOption match {
        case Some(record) =>
          // found a message, stopping, returning the message's offset
          stop = true
          nextMessageOffset = record.offset().some

        case None =>
          // no message, are we at the end of the partition?
          val (nextOffset, positionDuration) = durationOf { consumer.position(request.topicPartition) }

          positionCheckCount += 1
          positionTotalDurationMillis += positionDuration.toMillis

          // strictly speaking we could check for only for equality below...
          if (request.endOffset <= nextOffset) {
            stop = true
            lastOffset = nextOffset.some
          }
      }
    }
    consumer.assign(Seq.empty.asJava)

    val result = (nextMessageOffset, lastOffset) match {
      case (Some(nextMessageOffset), None) => ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(request, request.consumerGroupOffset, nextMessageOffset, messageAtTo = true)
      case (None, Some(lastOffset)) => ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(request, request.consumerGroupOffset, lastOffset, messageAtTo = false)
      case _ =>
        logger.warn(f"consumeAfterConsumerGroupOffset(${request.topicPartition}, ${request.consumerGroup}, ${request.consumerGroupOffset}) failed: exactly one of nextMessageOffset and lastOffset must be non-empty, but they were ($nextMessageOffset, $lastOffset)")
        ConsumeAfterConsumerGroupOffsetResult.Unknown(request)
    }
    (result, positionCheckCount, positionTotalDurationMillis)
  }

  private def consumeAfterConsumerGroupOffset(
    kafkaConnection: KafkaConnection,
    request: ConsumeAfterConsumerGroupOffset,
    pollDuration: java.time.Duration
  ): ConsumeAfterConsumerGroupOffsetResult = {
    val (result, duration) = durationOf {
      // we play it safe, not letting exceptions out
      Try {
        consumeAfterConsumerGroupOffset(
          createOrGetKafkaConsumer(kafkaConnection),
          request,
          pollDuration
        )
      }
    }

    result match {
      case Success((result, positionCheckCount, positionTotalDurationMillis)) =>
        metrics.timer(s"$kafkaClusterId:consumeafterconsumergroupoffset").update(duration)
        metrics.meter(s"$kafkaClusterId:succeededconsumeafterconsumergroupoffset").mark()
        logger.info(f"consumeAfterConsumerGroupOffset(${request.topicPartition}, ${request.consumerGroup}, ${request.consumerGroupOffset}) finished: $result (${duration.toMillisDouble}%.3f ms, positions queried $positionCheckCount times, took $positionTotalDurationMillis ms)")
        result

      case Failure(t) =>
        metrics.meter(s"$kafkaClusterId:failedconsumeafterconsumergroupoffset").mark()
        logger.warn(f"consumeAfterConsumerGroupOffset(${request.topicPartition}, ${request.consumerGroup}, ${request.consumerGroupOffset}) failed: (${duration.toMillisDouble}%.3f ms)", t)
        // Returning a proper NoMessagesBetweenOffsets but with from and to being the same and messageAtTo = true. This basically means, we don't know anything after the from offset, only that it's a message
        // (from was the consumer group offset, so it's a good guess).
        // This is good, because these failures won't be re-tried if the consumer group offset doesn't change (e.g. unknown compression codec for consumer groups that are stopped)
        ConsumeAfterConsumerGroupOffsetResult.NoMessagesBetweenOffsets(request, request.consumerGroupOffset, request.consumerGroupOffset, messageAtTo = true)
    }
  }

  private def createOrGetKafkaConsumer(kafkaConnection: KafkaConnection): KafkaConsumer[_, _] = {
    this.kafkaConsumer match {
      case Some(kafkaConsumer) => kafkaConsumer

      case None =>
        val kafkaProps = KafkaConfigProperties.forBytesConsumer(KafkaConsumerConfig(kafkaConnection, None))
        val kafkaConsumer = new KafkaConsumer[Bytes, Bytes](kafkaProps)
        this.kafkaConsumer = kafkaConsumer.some
        kafkaConsumer
    }
  }

  override def postStop(): Unit = {
    super.postStop()
    kafkaConsumer.foreach(_.close())
    kafkaConsumer = None
  }

  override def receive: Receive = {
    case SetKafkaConnection(kafkaConnection) =>
      this.kafkaConnection = kafkaConnection.some
      // needs to get rid of an existing kafka-consumer so that it's recreated when it's needed with the new kafkaConnection
      kafkaConsumer.foreach(_.close())
      kafkaConsumer = None

    case request: ConsumeAfterConsumerGroupOffset =>
      kafkaConnection match {
        case Some(kafkaConnection) => sender() ! consumeAfterConsumerGroupOffset(kafkaConnection, request, pollDuration)
        case None => sender() ! ConsumeAfterConsumerGroupOffsetResult.Unknown
      }
  }
}
