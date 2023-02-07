/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

import java.time.OffsetDateTime

import com.mwam.kafkakewl.domain.topology.ResetMode

/**
  * The possible positions in a topic partition where a consumer application can be reset to.
  */
sealed trait TopicPartitionPosition
object TopicPartitionPosition {
  final case class Default() extends TopicPartitionPosition
  final case class Beginning() extends TopicPartitionPosition
  final case class End() extends TopicPartitionPosition
  final case class Offset(value: Long) extends TopicPartitionPosition
  final case class TimeStamp(value: OffsetDateTime) extends TopicPartitionPosition

  def fromResetMode(reset: ResetMode): Option[TopicPartitionPosition] = reset match {
    case ResetMode.Beginning => Some(Beginning())
    case ResetMode.End => Some(End())
    case ResetMode.Ignore => None
  }
}

/**
  * Associates a partition and a position.
  */
final case class TopicPartitionPositionOfPartition(
  partition: Int,
  position: TopicPartitionPosition
)

/**
  * Associates a topic, a partition and a position.
  */
final case class TopicPartitionPositionOfTopicPartition(
  topic: String,
  partition: Int,
  position: TopicPartitionPosition
)

/**
  * Associates a topic, a partition and an offset.
  */
final case class OffsetOfTopicPartition(
  topic: String,
  partition: Int,
  offset: Long
)