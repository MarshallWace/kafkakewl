/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafka.config

object TopicConfigDefaults {
  def noReplicaPlacement(): Map[String, TopicConfigDefault] = Map(
    TopicConfigDefault.noReplicaPlacement()
  )

  def fromReplicaPlacement(
    replicaPlacement: String
  ): Map[String, TopicConfigDefault] = Map(
    TopicConfigDefault.fromReplicaPlacement(replicaPlacement)
  )

  def fromReplicaPlacement(
    replicaPlacement: String,
    minInsyncReplicas: Short,
    minInsyncReplicasOverridable: Boolean = false
  ): Map[String, TopicConfigDefault] = Map(
    TopicConfigDefault.fromReplicaPlacement(replicaPlacement),
    TopicConfigDefault.fromMinInsyncReplicas(minInsyncReplicas, minInsyncReplicasOverridable)
  )
}
