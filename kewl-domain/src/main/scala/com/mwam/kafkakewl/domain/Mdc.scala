/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology.TopologyEntityId

object Mdc {
  object Keys {
    val command = "command"
    val request = "request"
    val change = "change"
    val userName = "userName"
    val correlationId = "correlationId"
    val dryRun = "dryRun"
    val kafkaCluster = "kafkaClusterId"
    val topology = "topologyId"
  }

  def from(commandBase: CommandBase, metadata: CommandMetadata): Map[String, String] = Map(
    Keys.command -> commandBase.getClass.getSimpleName,
    Keys.change -> commandBase.canChangeState.toString,
    Keys.userName -> metadata.userName,
    Keys.correlationId -> metadata.correlationId,
    Keys.dryRun -> metadata.dryRun.toString
  )

  def fromKafkaClusterId(kafkaClusterId: KafkaClusterEntityId): Map[String, String] =
    Map(Keys.kafkaCluster -> kafkaClusterId.id)

  def fromKafkaClusterId(commandBase: CommandBase, metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId): Map[String, String] =
    Mdc.from(commandBase, metadata) + (Keys.kafkaCluster -> kafkaClusterId.id)

  def fromTopologyId(commandBase: CommandBase, metadata: CommandMetadata, topologyId: TopologyEntityId): Map[String, String] =
    Mdc.from(commandBase, metadata) + (Keys.topology -> topologyId.id)

  def fromKafkaClusterAndTopologyId(commandBase: CommandBase, metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId): Map[String, String] =
    Mdc.from(commandBase, metadata) + (Keys.kafkaCluster -> kafkaClusterId.id) + (Keys.topology -> topologyId.id)
}
