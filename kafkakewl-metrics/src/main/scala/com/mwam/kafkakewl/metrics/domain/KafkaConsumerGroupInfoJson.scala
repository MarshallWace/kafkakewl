/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder, JsonFieldDecoder, JsonFieldEncoder}

import scala.collection.immutable.{Map, SortedMap}

object KafkaConsumerGroupInfoJson {
  given JsonEncoder[KafkaConsumerGroupOffset] = DeriveJsonEncoder.gen[KafkaConsumerGroupOffset]
  given JsonDecoder[KafkaConsumerGroupOffset] = DeriveJsonDecoder.gen[KafkaConsumerGroupOffset]
  given JsonEncoder[KafkaConsumerGroupInfo] = DeriveJsonEncoder.gen[KafkaConsumerGroupInfo]
  given JsonDecoder[KafkaConsumerGroupInfo] = DeriveJsonDecoder.gen[KafkaConsumerGroupInfo]
  given JsonEncoder[SortedMap[Int, KafkaConsumerGroupOffset]] = JsonEncoder[Map[Int, KafkaConsumerGroupOffset]].contramap(_.toMap)
  given JsonDecoder[SortedMap[Int, KafkaConsumerGroupOffset]] = JsonDecoder[Map[Int, KafkaConsumerGroupOffset]].map(SortedMap.from)
}
