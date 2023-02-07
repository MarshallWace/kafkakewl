/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeploymentOptions}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.topology._

sealed trait KafkaClusterCommand extends CommandBase {
  val kafkaClusterId: KafkaClusterEntityId
  override lazy val mdc: Map[String, String] = Mdc.fromKafkaClusterId(this, metadata, kafkaClusterId)

  def succeededResult(response: KafkaClusterCommandResponse): KafkaClusterCommandResult.Succeeded =
    KafkaClusterCommandResult.Succeeded(metadata, kafkaClusterId, response)

  def succeededDeployResult(actions: Seq[KafkaClusterDeployCommandResponseAction], deployedTopologyChanged: Boolean): KafkaClusterCommandResult.Succeeded =
    KafkaClusterCommandResult.Succeeded(metadata, kafkaClusterId, KafkaClusterCommandResponse.Deployment(actions, deployedTopologyChanged))
  def failedResultWithDeployActions(deployActions: Seq[KafkaClusterDeployCommandResponseAction], deployedTopologyChanged: Boolean): KafkaClusterCommandResult.Failed =
    KafkaClusterCommandResult.Failed(metadata, kafkaClusterId, response = Some(KafkaClusterCommandResponse.Deployment(deployActions, deployedTopologyChanged)))

  def failedResult(reasons: Seq[CommandError]): KafkaClusterCommandResult.Failed =
    KafkaClusterCommandResult.Failed(metadata, kafkaClusterId, reasons)
  def failedResult(t: Throwable): KafkaClusterCommandResult.Failed =
    KafkaClusterCommandResult.Failed(metadata, kafkaClusterId, Seq(CommandError.exception(t)))
  def failedResult(reason: CommandError): KafkaClusterCommandResult.Failed = failedResult(Seq(reason))
  def notImplementedResult: KafkaClusterCommandResult.Failed = failedResult(CommandError.notImplementedError())
}
object KafkaClusterCommand {
  final case class Diff(
    metadata: CommandMetadata,
    kafkaClusterId: KafkaClusterEntityId,
    ignoreNonKewlKafkaResources: Boolean
  ) extends KafkaClusterCommand

  sealed trait KafkaClusterTopologyIdCommand {
    this: KafkaClusterCommand =>
    val topologyId: TopologyEntityId
    override lazy val mdc: Map[String, String] = Mdc.fromKafkaClusterAndTopologyId(this, metadata, kafkaClusterId, topologyId)
  }

  final case class DeployTopology(
    metadata: CommandMetadata,
    kafkaClusterId: KafkaClusterEntityId,
    topologyId: TopologyEntityId,
    topologyVersion: Int,
    deploymentVersion: Int,
    topologyToDeploy: TopologyToDeploy,
    topologiesStateForAuthorizationCode: String,
    options: DeploymentOptions
  ) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with CanChangeStateCommand

  final case class UndeployTopology(
    metadata: CommandMetadata,
    kafkaClusterId: KafkaClusterEntityId,
    topologyId: TopologyEntityId,
    deploymentVersion: Option[Int],
    topologyVersion: Option[Int], // only for permission-checking (we can't always set it hence it's an option)
    topologiesStateForAuthorizationCode: String,
    options: DeploymentOptions
  ) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with CanChangeStateCommand

  final case class DeployedTopologyReset(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, options: DeployedTopology.ResetOptions) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with CanChangeStateCommand

  sealed trait DeployedTopologyReadOperation {
    val topologyId: TopologyEntityId
  }
  final case class DeployedTopologyTopicGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with DeployedTopologyReadOperation
  final case class DeployedTopologyTopicGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, topicId: LocalTopicId) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with DeployedTopologyReadOperation
  final case class DeployedTopologyApplicationGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with DeployedTopologyReadOperation
  final case class DeployedTopologyApplicationGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, applicationId: LocalApplicationId) extends KafkaClusterCommand with KafkaClusterTopologyIdCommand with DeployedTopologyReadOperation
}
