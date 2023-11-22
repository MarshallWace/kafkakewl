/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import sttp.tapir.*
import sttp.tapir.generic.auto.*

import scala.collection.immutable.SortedMap

object KafkaConsumerGroupInfoSchema {
  given Schema[KafkaConsumerGroupInfo] = Schema.derived[KafkaConsumerGroupInfo]
  // TODO hmmm... this looks nasty, any better way of doing this?
  given Schema[SortedMap[Int, KafkaConsumerGroupOffset]] = Schema
    .schemaForMap[Int, KafkaConsumerGroupOffset](_.toString)
    .map(a => Some(SortedMap.from(a)))(_.toMap)
}
