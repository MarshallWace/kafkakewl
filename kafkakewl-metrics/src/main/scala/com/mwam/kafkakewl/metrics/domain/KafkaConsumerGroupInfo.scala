/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import scala.collection.immutable.SortedMap

final case class KafkaConsumerGroupInfo(
    topics: SortedMap[String, KafkaConsumerGroupTopicInfo]
)

final case class KafkaConsumerGroupTopicInfo(
    partitions: SortedMap[Int, KafkaConsumerGroupMetrics]
)

object KafkaConsumerGroupInfo {
  val empty: KafkaConsumerGroupInfo = KafkaConsumerGroupInfo(SortedMap.empty)
}
