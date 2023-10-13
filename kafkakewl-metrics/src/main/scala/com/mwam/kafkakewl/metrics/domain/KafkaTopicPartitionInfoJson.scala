/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder, JsonFieldDecoder, JsonFieldEncoder}

import scala.collection.immutable.SortedMap

object KafkaTopicPartitionInfoJson {
  given JsonEncoder[KafkaTopicPartitionInfo] = DeriveJsonEncoder.gen[KafkaTopicPartitionInfo]
  given JsonDecoder[KafkaTopicPartitionInfo] = DeriveJsonDecoder.gen[KafkaTopicPartitionInfo]
  given JsonEncoder[KafkaSingleTopicPartitionInfos] = JsonEncoder[Map[Int, KafkaTopicPartitionInfo]].contramap(_.toMap)
  given JsonDecoder[KafkaSingleTopicPartitionInfos] = JsonDecoder[Map[Int, KafkaTopicPartitionInfo]].map(SortedMap.from)
}
