/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.deploy._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionEntityId}
import com.mwam.kafkakewl.domain.topology._

import scala.reflect.runtime.universe._

/**
  * A base trait for commands.
  */
sealed trait Command extends CommandBase {
  def succeededResult(
    response: CommandResponse = CommandResponse.None(),
    entityStateChanged: Boolean = false
  ): CommandResult.Succeeded = CommandResult.Succeeded(metadata, response, entityStateChanged = entityStateChanged)

  def succeededResult[E <: Entity : TypeTag](
    entityState: EntityState.Live[E],
    compact: Boolean
  )(
    implicit compactor: EntityCompactor[E]
  ): CommandResult.Succeeded = {
    if (compact) {
      val compactedEntityState = EntityState.Live[compactor.Compacted](entityState.metadata, compactor.compact(entityState.entity))
      succeededResult(CommandResponse.State(compactedEntityState)(compactor.typeTag))
    } else {
      succeededResult(CommandResponse.State(entityState))
    }
  }

  def succeededResult[E <: Entity : TypeTag](
    entityStates: IndexedSeq[EntityState.Live[E]],
    compact: Boolean
  )(
    implicit compactor: EntityCompactor[E]
  ): CommandResult.Succeeded = {
    if (compact) {
      val compactedEntityStates = entityStates.map(s => EntityState.Live[compactor.Compacted](s.metadata, compactor.compact(s.entity)))
      succeededResult(CommandResponse.StateList(compactedEntityStates)(compactor.typeTag))
    } else {
      succeededResult(CommandResponse.StateList(entityStates))
    }
  }

  def failedResult(reasons: Seq[CommandError]): CommandResult.Failed = CommandResult.Failed(metadata, reasons)
  def failedResult(reason: CommandError): CommandResult.Failed = failedResult(Seq(reason))
  def validationFailedResult(message: String): CommandResult.Failed = failedResult(CommandError.validationError(message))
}
object Command {
  // for commands referring to a kafka-cluster
  sealed trait KafkaClusterIdCommand {
    this: CommandBase =>
    val kafkaClusterId: KafkaClusterEntityId
    override lazy val mdc: Map[String, String] = Mdc.fromKafkaClusterId(this, metadata, kafkaClusterId)
  }

  // trait for commands that should be directed to the kafka-cluster
  sealed trait ToKafkaClusterCommand {
    this: CommandBase =>
    def createMetadataFrom: CommandMetadata = metadata.withTimeStampUtcNow().withNewCorrelationId()
    def toKafkaClusterCommand: KafkaClusterCommand
  }

  // health-check commands
  final case class HealthIsLive(metadata: CommandMetadata) extends Command
  final case class HealthIsReady(metadata: CommandMetadata) extends Command
  final case class HealthIsLiveResponse(live: Boolean, errors: Seq[String]) {
    def withErrorPrefix(prefix: String): HealthIsLiveResponse = copy(errors = errors.map(error => s"$prefix$error"))
    def +(other: HealthIsLiveResponse): HealthIsLiveResponse = HealthIsLiveResponse(other.live && live, other.errors ++ errors)
  }
  object HealthIsLiveResponse {
    def apply(throwable: Throwable): HealthIsLiveResponse = HealthIsLiveResponse(live = false, Seq(throwable.getMessage))
  }

  final case class HealthIsReadyResponse(ready: Boolean, errors: Seq[String]) {
    def withErrorPrefix(prefix: String): HealthIsReadyResponse = copy(errors = errors.map(error => s"$prefix$error"))
    def +(other: HealthIsReadyResponse): HealthIsReadyResponse = HealthIsReadyResponse(other.ready && ready, other.errors ++ errors)
  }
  object HealthIsReadyResponse {
    def apply(throwable: Throwable): HealthIsReadyResponse = HealthIsReadyResponse(ready = false, Seq(throwable.getMessage))
  }

  // admin commands
  sealed trait AdminCommand extends Command
  final case class ReInitialize(metadata: CommandMetadata, wipe: Boolean) extends AdminCommand with CanChangeStateCommand
  final case class KafkaDiff(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, ignoreNonKewlKafkaResources: Boolean)
    extends AdminCommand with KafkaClusterIdCommand with ToKafkaClusterCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.Diff(createMetadataFrom, kafkaClusterId, ignoreNonKewlKafkaResources)
  }
  final case class PermissionPluginGetUserPermissions(metadata: CommandMetadata, userNameOfPermissions: String) extends AdminCommand
  final case class PermissionPluginInvalidateCachedUserPermissions(metadata: CommandMetadata, userNameOfPermissions: Option[String]) extends AdminCommand with CanChangeStateCommand

  sealed trait StateCommand extends Command
  sealed trait StateReadOnlyCommand extends StateCommand

  // topology commands
  sealed trait TopologyEntityCommand extends StateCommand
  // for commands with a topology id
  sealed trait TopologyIdCommand {
    this: CommandBase =>
    val topologyId: TopologyEntityId
    override lazy val mdc: Map[String, String] = Mdc.fromTopologyId(this, metadata, topologyId)
  }
  final case class TopologyGetAll(metadata: CommandMetadata, compact: Boolean) extends TopologyEntityCommand with StateReadOnlyCommand
  final case class TopologyGet(metadata: CommandMetadata, topologyId: TopologyEntityId, compact: Boolean, version: Option[Int] = None) extends TopologyEntityCommand with TopologyIdCommand with StateReadOnlyCommand
  final case class TopologyCreate(metadata: CommandMetadata, topologyId: TopologyEntityId, topology: Topology) extends TopologyEntityCommand with TopologyIdCommand with CanChangeStateCommand
  final case class TopologyUpdate(metadata: CommandMetadata, topologyId: TopologyEntityId, topology: Topology) extends TopologyEntityCommand with TopologyIdCommand with CanChangeStateCommand
  final case class TopologyDelete(metadata: CommandMetadata, topologyId: TopologyEntityId) extends TopologyEntityCommand with TopologyIdCommand with CanChangeStateCommand

  // permission commands
  sealed trait PermissionEntityCommand extends StateCommand
  final case class PermissionGetAll(metadata: CommandMetadata, compact: Boolean) extends PermissionEntityCommand with StateReadOnlyCommand
  final case class PermissionGet(metadata: CommandMetadata, permissionId: PermissionEntityId, compact: Boolean) extends PermissionEntityCommand with StateReadOnlyCommand
  final case class PermissionCreate(metadata: CommandMetadata, permissionId: PermissionEntityId, permission: Permission) extends PermissionEntityCommand with CanChangeStateCommand
  final case class PermissionCreateWithContent(metadata: CommandMetadata, permission: Permission) extends PermissionEntityCommand with CanChangeStateCommand
  final case class PermissionUpdate(metadata: CommandMetadata, permissionId: PermissionEntityId, permission: Permission) extends PermissionEntityCommand with CanChangeStateCommand
  final case class PermissionDelete(metadata: CommandMetadata, permissionId: PermissionEntityId) extends PermissionEntityCommand with CanChangeStateCommand
  final case class PermissionDeleteByContent(metadata: CommandMetadata, permission: Permission) extends PermissionEntityCommand with CanChangeStateCommand

  // kafka-cluster commands
  sealed trait KafkaClusterEntityCommand extends StateCommand
  final case class KafkaClusterGetAll(metadata: CommandMetadata, compact: Boolean) extends KafkaClusterEntityCommand with StateReadOnlyCommand
  final case class KafkaClusterGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, compact: Boolean) extends KafkaClusterEntityCommand with KafkaClusterIdCommand with StateReadOnlyCommand
  final case class KafkaClusterCreate(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster) extends KafkaClusterEntityCommand with KafkaClusterIdCommand with CanChangeStateCommand
  final case class KafkaClusterUpdate(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster) extends KafkaClusterEntityCommand with KafkaClusterIdCommand with CanChangeStateCommand
  final case class KafkaClusterDelete(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId) extends KafkaClusterEntityCommand with KafkaClusterIdCommand with CanChangeStateCommand

  // for commands referring to a kafka-cluster and a topology
  sealed trait KafkaClusterIdWithTopologyIdCommand extends KafkaClusterIdCommand {
    this: CommandBase =>
    val topologyId: TopologyEntityId
    override lazy val mdc: Map[String, String] = Mdc.fromKafkaClusterAndTopologyId(this, metadata, kafkaClusterId, topologyId)
  }

  // deployment commands
  sealed trait DeploymentEntityCommand extends StateCommand
  sealed trait DeploymentIdCommand extends KafkaClusterIdWithTopologyIdCommand {
    this: CommandBase =>
    val deploymentId: DeploymentEntityId = DeploymentEntityId(KafkaClusterAndTopology.id(kafkaClusterId, topologyId))
  }

  sealed trait DeploymentCreateOrUpdateCommand extends DeploymentEntityCommand with DeploymentIdCommand {
    val deployment: DeploymentChange
  }
  final case class DeploymentGetAll(metadata: CommandMetadata, compact: Boolean) extends DeploymentEntityCommand with StateReadOnlyCommand
  final case class DeploymentGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, compact: Boolean) extends DeploymentEntityCommand with DeploymentIdCommand with StateReadOnlyCommand
  // Note that the create/update commands take a DeploymentChange field which is slightly different from the actual Deployment
  // The idea is that the DeploymentChange gives the processors more information that just the new state of the Deployment (e.g. allowed unsafe changes or LatestOnce topology version).
  final case class DeploymentCreate(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, deployment: DeploymentChange) extends DeploymentCreateOrUpdateCommand with CanChangeStateCommand
  final case class DeploymentUpdate(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, deployment: DeploymentChange) extends DeploymentCreateOrUpdateCommand with CanChangeStateCommand
  final case class DeploymentDelete(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId) extends DeploymentEntityCommand with DeploymentIdCommand with CanChangeStateCommand
  final case class DeploymentReApply(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, options: DeploymentOptions = DeploymentOptions()) extends DeploymentEntityCommand with DeploymentIdCommand with CanChangeStateCommand

  // deployed-topology commands
  sealed trait DeployedTopologyEntityCommand extends Command
  sealed trait KafkaClusterDeployedTopologyEntityCommand extends DeployedTopologyEntityCommand with KafkaClusterIdCommand
  sealed trait DeployedTopologyIdCommand extends KafkaClusterIdWithTopologyIdCommand {
    this: CommandBase =>
    val deployedTopologyId: DeployedTopologyEntityId = DeployedTopologyEntityId(KafkaClusterAndTopology.id(kafkaClusterId, topologyId))
  }
  final case class DeployedTopologyGetKafkaClustersAll(metadata: CommandMetadata, compact: Boolean) extends DeployedTopologyEntityCommand with StateReadOnlyCommand
  final case class DeployedTopologyGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, compact: Boolean) extends KafkaClusterDeployedTopologyEntityCommand with StateReadOnlyCommand
  final case class DeployedTopologyGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, compact: Boolean) extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with StateReadOnlyCommand

  final case class DeployedTopologiesGetResolved(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologies: Seq[FlexibleName]) extends KafkaClusterDeployedTopologyEntityCommand with StateReadOnlyCommand

  // deployed-topology-metrics commands
  sealed trait DeployedTopologyMetricsCommand
  final case class DeployedTopologyMetricsGetKafkaClustersAll(metadata: CommandMetadata, compact: Boolean) extends DeployedTopologyEntityCommand with DeployedTopologyMetricsCommand with StateReadOnlyCommand
  final case class DeployedTopologyMetricsGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, compact: Boolean) extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyMetricsCommand with StateReadOnlyCommand
  final case class DeployedTopologyMetricsGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, compact: Boolean) extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with DeployedTopologyMetricsCommand with StateReadOnlyCommand

  // deployed-topology commands which are sent to the kafka-cluster right away
  final case class DeployedTopologyReset(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, options: DeployedTopology.ResetOptions)
    extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with ToKafkaClusterCommand with CanChangeStateCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.DeployedTopologyReset(createMetadataFrom, kafkaClusterId, topologyId, options)
  }

  final case class DeployedTopologyTopicGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId)
    extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with ToKafkaClusterCommand with StateReadOnlyCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.DeployedTopologyTopicGetAll(createMetadataFrom, kafkaClusterId, topologyId)
  }

  final case class DeployedTopologyTopicGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, topicId: LocalTopicId)
    extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with ToKafkaClusterCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.DeployedTopologyTopicGet(createMetadataFrom, kafkaClusterId, topologyId, topicId)
  }

  final case class DeployedTopologyApplicationGetAll(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId)
    extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with ToKafkaClusterCommand with StateReadOnlyCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.DeployedTopologyApplicationGetAll(createMetadataFrom, kafkaClusterId, topologyId)
  }

  final case class DeployedTopologyApplicationGet(metadata: CommandMetadata, kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId, applicationId: LocalApplicationId)
    extends KafkaClusterDeployedTopologyEntityCommand with DeployedTopologyIdCommand with ToKafkaClusterCommand {
    def toKafkaClusterCommand = KafkaClusterCommand.DeployedTopologyApplicationGet(createMetadataFrom, kafkaClusterId, topologyId, applicationId)
  }
}
