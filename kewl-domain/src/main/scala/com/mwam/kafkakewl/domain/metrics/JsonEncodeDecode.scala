/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package metrics

import cats.syntax.functor._
import com.mwam.kafkakewl.domain.kafka.KafkaTopic
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}
import io.circe.syntax._

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._

  implicit val consumerGroupStatusEncoder: Encoder[ConsumerGroupStatus] = deriveEnumerationEncoder
  implicit val consumerGroupStatusDecoder: Decoder[ConsumerGroupStatus] = deriveEnumerationDecoder

  implicit val partitionHighOffsetEncoder: Encoder[PartitionHighOffset] = deriveConfiguredEncoder
  implicit val partitionHighOffsetDecoder: Decoder[PartitionHighOffset] = deriveConfiguredDecoder
  implicit val consumerGroupOffsetEncoder: Encoder[ConsumerGroupOffset] = deriveConfiguredEncoder
  implicit val consumerGroupOffsetDecoder: Decoder[ConsumerGroupOffset] = deriveConfiguredDecoder
  implicit val consumerGroupMetricsEncoder: Encoder[ConsumerGroupMetrics] = deriveConfiguredEncoder
  implicit val consumerGroupMetricsDecoder: Decoder[ConsumerGroupMetrics] = deriveConfiguredDecoder

  implicit val topicPartitionsConsumerGroupMetricsOrErrorEncoder: Encoder[Either[String, TopicPartitionsConsumerGroupMetrics]] = Encoder.instance {
    case Left(e) => Map("error" -> e).asJson
    case Right(v) => v.asJson
  }
  implicit val topicPartitionsConsumerGroupMetricsOrErrorDecoder: Decoder[Either[String, TopicPartitionsConsumerGroupMetrics]] =
    List[Decoder[Either[String, TopicPartitionsConsumerGroupMetrics]]](
      Decoder[TopicPartitionsConsumerGroupMetrics].map(Right(_)).widen,
      Decoder[Map[String, String]].map(a => Left(a.getOrElse("error", "???"))).widen
    ).reduce(_ or _)


  implicit val topicInfoOrErrorEncoder: Encoder[Either[String, KafkaTopic.Info]] = Encoder.instance {
    case Left(e) => Map("error" -> e).asJson
    case Right(v) => v.asJson
  }
  implicit val topicInfoOrErrorDecoder: Decoder[Either[String, KafkaTopic.Info]] =
    List[Decoder[Either[String, KafkaTopic.Info]]](
      Decoder[KafkaTopic.Info].map(Right(_)).widen,
      Decoder[Map[String, String]].map(a => Left(a.getOrElse("error", "???"))).widen
    ).reduce(_ or _)
}
