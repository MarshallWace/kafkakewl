/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafka

import io.circe.generic.extras.semiauto._
import io.circe.{Decoder, Encoder}

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._

  implicit val kafkaConsumerGroupStateEncoder: Encoder[KafkaConsumerGroup.State] = deriveEnumerationEncoder
  implicit val kafkaConsumerGroupStateDecoder: Decoder[KafkaConsumerGroup.State] = deriveEnumerationDecoder

  implicit val kafkaTopicMetricsEncoder: Encoder[KafkaTopic.Metrics] = deriveConfiguredEncoder
  implicit val kafkaTopicMetricsDecoder: Decoder[KafkaTopic.Metrics] = deriveConfiguredDecoder
  implicit val kafkaTopicPartitionInfoEncoder: Encoder[KafkaTopic.PartitionInfo] = deriveConfiguredEncoder
  implicit val kafkaTopicPartitionInfoDecoder: Decoder[KafkaTopic.PartitionInfo] = deriveConfiguredDecoder
  implicit val kafkaTopicInfoEncoder: Encoder[KafkaTopic.Info] = deriveConfiguredEncoder
  implicit val kafkaTopicInfoDecoder: Decoder[KafkaTopic.Info] = deriveConfiguredDecoder
}
