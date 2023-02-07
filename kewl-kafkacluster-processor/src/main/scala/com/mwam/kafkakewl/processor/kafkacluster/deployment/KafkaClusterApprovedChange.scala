/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain.KafkaClusterCommandActionSafety
import com.mwam.kafkakewl.domain.deploy.UnsafeKafkaClusterChangeDescription

private[kafkacluster] final case class KafkaClusterApprovedChange(
  plannedChange: KafkaClusterChange,
  safety: KafkaClusterCommandActionSafety,
  notAllowedUnsafeChange: Option[UnsafeKafkaClusterChangeDescription],
  allowedChange: Option[KafkaClusterChange]
) {
  def toTuple: (String, KafkaClusterApprovedChange) = (plannedChange.key, this)
}

private[kafkacluster] object KafkaClusterApprovedChange {
  def fromKafkaClusterChange(
    isUnsafeChangeAllowed: UnsafeKafkaClusterChangeDescription => Boolean
  )(plannedChange: KafkaClusterChange): KafkaClusterApprovedChange = {
    val unsafeChangeDescription = plannedChange.unsafeChange
    val unsafeChangeIsAllowedOrNone = unsafeChangeDescription.map(isUnsafeChangeAllowed)
    val safeChange = plannedChange.makeSafe
    val safety = (unsafeChangeIsAllowedOrNone, safeChange) match {
      case (Some(true), _) => KafkaClusterCommandActionSafety.UnsafeAllowed
      case (Some(false), Some(_)) => KafkaClusterCommandActionSafety.UnsafePartiallyAllowed
      case (Some(false), None) => KafkaClusterCommandActionSafety.UnsafeNotAllowed
      case (None, _) => KafkaClusterCommandActionSafety.Safe
    }

    val unsafeChangeIsAllowed = unsafeChangeIsAllowedOrNone.getOrElse(true)
    KafkaClusterApprovedChange(
      plannedChange,
      safety,
      if (unsafeChangeIsAllowed) None else unsafeChangeDescription,
      if (unsafeChangeIsAllowed) Some(plannedChange) else safeChange
    )
  }

  def fromKafkaClusterChanges(
    isUnsafeChangeAllowed: UnsafeKafkaClusterChangeDescription => Boolean
  )(plannedChanges: Seq[KafkaClusterChange]): Seq[KafkaClusterApprovedChange] = {
    val fromKafkaClusterChangeFn = fromKafkaClusterChange(isUnsafeChangeAllowed) _
    plannedChanges.map(fromKafkaClusterChangeFn)
  }
}