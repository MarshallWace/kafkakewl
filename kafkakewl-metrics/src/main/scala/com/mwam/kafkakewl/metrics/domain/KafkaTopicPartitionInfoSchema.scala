/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import sttp.tapir.*
import sttp.tapir.generic.auto.*

import scala.collection.immutable.SortedMap

object KafkaTopicPartitionInfoSchema {
  // TODO hmmm... this looks nasty, any better way of doing this?
  given Schema[KafkaSingleTopicPartitionInfos] = Schema.schemaForMap[Int, KafkaTopicPartitionInfo](_.toString).map(a => Some(SortedMap.from(a)))(_.toMap)
}
