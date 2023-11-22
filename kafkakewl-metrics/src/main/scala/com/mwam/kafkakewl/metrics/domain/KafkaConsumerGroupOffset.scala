/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import java.time.OffsetDateTime

final case class KafkaConsumerGroupOffset(
    offset: Long,
    metadata: String,
    lastOffsetsTimestampUtc: OffsetDateTime
) {
  override def toString: String =
    if (metadata == null || metadata.isEmpty)
      s"$offset@${lastOffsetsTimestampUtc.toString}"
    else s"$offset['$metadata']@${lastOffsetsTimestampUtc.toString}"
}

type KafkaConsumerGroupOffsets =
  Map[ConsumerGroupTopicPartition, Option[KafkaConsumerGroupOffset]]

object KafkaConsumerGroupOffsets {
  val empty
      : Map[ConsumerGroupTopicPartition, Option[KafkaConsumerGroupOffset]] =
    Map.empty
}
