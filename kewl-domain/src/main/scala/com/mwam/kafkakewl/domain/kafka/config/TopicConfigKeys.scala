/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafka.config

object TopicConfigKeys {
  val confluentPlacementConstraints = "confluent.placement.constraints"
  val minInsyncReplicas = "min.insync.replicas"
  val retentionMs = "retention.ms"
}
