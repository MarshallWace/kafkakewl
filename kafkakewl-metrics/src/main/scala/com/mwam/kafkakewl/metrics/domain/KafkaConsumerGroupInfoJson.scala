/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder, JsonFieldDecoder, JsonFieldEncoder}

import scala.collection.immutable.{Map, SortedMap}
import KafkaTopicPartitionInfoJson.given

object KafkaConsumerGroupInfoJson {
  given JsonEncoder[KafkaConsumerGroupOffset] = DeriveJsonEncoder.gen[KafkaConsumerGroupOffset]
  given JsonDecoder[KafkaConsumerGroupOffset] = DeriveJsonDecoder.gen[KafkaConsumerGroupOffset]
  given JsonEncoder[ConsumerGroupStatus] = DeriveJsonEncoder.gen[ConsumerGroupStatus]
  given JsonDecoder[ConsumerGroupStatus] = DeriveJsonDecoder.gen[ConsumerGroupStatus]
  given JsonEncoder[KafkaConsumerGroupMetrics] = DeriveJsonEncoder.gen[KafkaConsumerGroupMetrics]
  given JsonDecoder[KafkaConsumerGroupMetrics] = DeriveJsonDecoder.gen[KafkaConsumerGroupMetrics]
  given kafkaConsumerGroupTopicInfoMapEncoder: JsonEncoder[SortedMap[Int, KafkaConsumerGroupMetrics]] =
    JsonEncoder[Map[Int, KafkaConsumerGroupMetrics]].contramap(_.unsorted)
  given kafkaConsumerGroupTopicInfoMapDecoder: JsonDecoder[SortedMap[Int, KafkaConsumerGroupMetrics]] =
    JsonDecoder[Map[Int, KafkaConsumerGroupMetrics]].map(SortedMap.from)
  given JsonEncoder[KafkaConsumerGroupTopicInfo] = DeriveJsonEncoder.gen[KafkaConsumerGroupTopicInfo]
  given JsonDecoder[KafkaConsumerGroupTopicInfo] = DeriveJsonDecoder.gen[KafkaConsumerGroupTopicInfo]
  given JsonEncoder[SortedMap[String, KafkaConsumerGroupTopicInfo]] = JsonEncoder[Map[String, KafkaConsumerGroupTopicInfo]].contramap(_.unsorted)
  given JsonDecoder[SortedMap[String, KafkaConsumerGroupTopicInfo]] = JsonDecoder[Map[String, KafkaConsumerGroupTopicInfo]].map(SortedMap.from)
  given JsonEncoder[KafkaConsumerGroupInfo] = DeriveJsonEncoder.gen[KafkaConsumerGroupInfo]
  given JsonDecoder[KafkaConsumerGroupInfo] = DeriveJsonDecoder.gen[KafkaConsumerGroupInfo]
}
