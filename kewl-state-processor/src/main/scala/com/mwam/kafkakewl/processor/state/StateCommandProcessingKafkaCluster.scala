/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import cats.data.ValidatedNel
import cats.syntax.validated._
import cats.syntax.traverse._
import cats.syntax.either._
import cats.instances.vector._
import cats.data.Validated._
import com.mwam.kafkakewl.common.AllStateEntities.ReadableVersionedStateStores._
import com.mwam.kafkakewl.common.{AllStateEntities, ApplyStateChange, ReadableVersionedStateStore}
import com.mwam.kafkakewl.domain._
import StateCommandProcessingResult.EntityChangesWithKafkaClusterCommands
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentOptions, DeploymentStateChange, DeploymentTopologyVersion}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterAndTopology, KafkaClusterEntityId, KafkaClusterStateChange}
import com.mwam.kafkakewl.domain.permission.{Permission, PermissionStateChange}
import com.mwam.kafkakewl.domain.topology.{Namespace, Topology, TopologyEntityId, TopologyStateChange}
import com.typesafe.scalalogging.LazyLogging

private[state] object StateCommandProcessingKafkaCluster extends LazyLogging {
  /**
    * Fail-fast result for the kafka-cluster command generation. It's either a CommandResult.Failed or the EntityChangesWithKafkaClusterCommands above.
    */
  type KafkaClusterCommandFuncResult = Either[CommandResult.Failed, EntityChangesWithKafkaClusterCommands]
  object KafkaClusterCommandFuncResult {
    def success(
      entityChanges: AllStateEntitiesStateChanges = AllStateEntitiesStateChanges(),
      kafkaClusterCommands: Seq[KafkaClusterCommand] = Seq.empty[KafkaClusterCommand]
    ): KafkaClusterCommandFuncResult = Right(EntityChangesWithKafkaClusterCommands(entityChanges, kafkaClusterCommands))
    def failure(failed: CommandResult.Failed): KafkaClusterCommandFuncResult = Left(failed)
  }

  private final case class TopologyDeploymentKey(kafkaClusterId: KafkaClusterEntityId, topologyId: TopologyEntityId)
  private final case class TopologyDeployment(
    kafkaClusterId: KafkaClusterEntityId,
    kafkaCluster: KafkaCluster,
    topologyId: TopologyEntityId,
    topologyVersion: Int,
    deploymentVersion: Int,
    topology: Topology
  ) {
    val id: String = KafkaClusterAndTopology.id(kafkaClusterId, topologyId)
  }

  private def resolveDeploymentTopologyVersion(
    topologyStateStore: ReadableVersionedStateStore[Topology],
    deployment: Deployment
  ): Option[Int] = {
    deployment.topologyVersion match {
      case DeploymentTopologyVersion.Remove() => None
      case DeploymentTopologyVersion.Exact(exact) => Some(exact)
      case DeploymentTopologyVersion.LatestTracking() => topologyStateStore.getLatestLiveState(deployment.topologyId).map(_.version)
    }
  }

  private def getDeployedTopology(
    stateStores: AllStateEntities.ReadableVersionedStateStores,
    kafkaClusterId: KafkaClusterEntityId,
    topologyId: TopologyEntityId,
    topologyVersion: Int,
    deploymentVersion: Int
  ): Option[(TopologyDeploymentKey, TopologyDeployment)] = {
    for {
      topologyState <- stateStores.topology.getStateVersions(topologyId).find(_.version == topologyVersion)
      kafkaCluster <- stateStores.kafkaCluster.getLatestLiveState(kafkaClusterId)
      topology <- topologyState match {
        case v: EntityState.Live[Topology] => Some(v.entity)
        // this is really an error, the previous validation should have spotted it (a deployment referencing a topology version that's deleted)
        case _: EntityState.Deleted[Topology] => None
      }
    } yield (TopologyDeploymentKey(kafkaClusterId, topologyId), TopologyDeployment(kafkaClusterId, kafkaCluster.entity, topologyId, topologyVersion, deploymentVersion, topology))
  }

  private def getDeployedTopologies(
    stateStores: AllStateEntities.ReadableVersionedStateStores
  ): Map[TopologyDeploymentKey, TopologyDeployment] = {
    stateStores.deployment.getLatestLiveStates
      .flatMap(d => {
        val kafkaClusterId = d.entity.kafkaClusterId
        val topologyId = d.entity.topologyId
        for {
          topologyVersion <- resolveDeploymentTopologyVersion(stateStores.topology, d.entity)
          deployedTopology <- getDeployedTopology(stateStores, kafkaClusterId, topologyId, topologyVersion, d.version)
        } yield deployedTopology
      }).toMap
  }

  private def diffDeployedTopologies(
    before: Map[TopologyDeploymentKey, TopologyDeployment],
    after: Map[TopologyDeploymentKey, TopologyDeployment]
  ): (Seq[TopologyDeployment], Seq[TopologyDeployment], Seq[(TopologyDeployment, TopologyDeployment)]) = {
    val missingFromBefore = (after.keySet -- before.keySet).toSeq.map(after(_))
    val missingFromAfter = (before.keySet -- after.keySet).toSeq.map(before(_))
    val changesFromBeforeToAfter = (before.keySet intersect after.keySet)
      .toSeq
      .map(dpk => (before(dpk), after(dpk)))
      .filter { case (b, a) => b.topologyVersion != a.topologyVersion }

    (missingFromBefore, missingFromAfter, changesFromBeforeToAfter)
  }

  private def getTopologiesStateOfNamespaceForAuthorizationCode(
    afterStateStores: AllStateEntities.ReadableVersionedStateStores,
    namespace: Namespace
  ): String = {
      // trying to find all other topologies latest versions with this namespace (but possibly a different topology to support batches too)
      afterStateStores.topology.getLatestLiveStates
        .filter(_.entity.namespace == namespace)
        .map(s => (s.metadata.id, s.metadata.version))
        .sorted
        .map { case (id, version) => s"$id:$version" }
        .mkString(",")
  }

  private def getTopologiesStateForAuthorizationCode(
    afterStateStores: AllStateEntities.ReadableVersionedStateStores,
    topologyId: TopologyEntityId
  ): String = {
    // if we don't know the namespace, we try to get it from the latest version of the given topology id
    afterStateStores.topology.getLatestLiveState(topologyId)
      .map(s => getTopologiesStateOfNamespaceForAuthorizationCode(afterStateStores, s.entity.namespace))
      // if there is no latest, we just use the topology-id as the state (the authorization code won't work across different topologies in the namespace)
      .getOrElse(topologyId.id)
  }

  def getKafkaClusterCommands(
    command: Command,
    beforeStateStores: AllStateEntities.ReadableVersionedStateStores,
    changes: AllStateEntitiesStateChanges = AllStateEntitiesStateChanges()
  )(implicit
    permissionApplyStateChange: ApplyStateChange[Permission, PermissionStateChange.StateChange],
    topologyApplyStateChange: ApplyStateChange[Topology, TopologyStateChange.StateChange],
    kafkaClusterApplyStateChange: ApplyStateChange[KafkaCluster, KafkaClusterStateChange.StateChange],
    deploymentApplyStateChange: ApplyStateChange[Deployment, DeploymentStateChange.StateChange]
  ): KafkaClusterCommandFuncResult  = {

    // some commands contain information about allowed-unsafe-kafka-cluster-changes and other deployment options,
    // we extract that here so that we can pass it to the kafka-cluster processor
    val deploymentOptions = command match {
      case c: Command.DeploymentReApply => c.options
      case c: Command.DeploymentCreateOrUpdateCommand => c.deployment.options
      case _ => DeploymentOptions()
    }

    def deployTopologyCommand(topologiesStateForAuthorizationCode: String)(
      deployedTopology: TopologyDeployment
    ): ValidatedNel[CommandError, KafkaClusterCommand] = {
      val deploymentEnvironmentVariables = TopologyDeployable.deploymentVariables(
        deployedTopology.kafkaClusterId,
        deployedTopology.kafkaCluster,
        deployedTopology.topology
      )
      TopologyDeployable.makeTopologyDeployable(deploymentEnvironmentVariables, deployedTopology.topology)
        .map(topologyToDeploy =>
          KafkaClusterCommand.DeployTopology(
            command.metadata.withTimeStampUtcNow().withNewCorrelationId(),
            deployedTopology.kafkaClusterId,
            deployedTopology.topologyId,
            deployedTopology.topologyVersion,
            deployedTopology.deploymentVersion,
            topologyToDeploy,
            topologiesStateForAuthorizationCode,
            deploymentOptions
          )
        )
    }

    def undeployTopologyCommand(topologiesStateForAuthorizationCode: String)(
      kafkaClusterId: KafkaClusterEntityId,
      topologyId: TopologyEntityId,
      deploymentVersion: Option[Int],
      topologyVersion: Option[Int] = None
    ): ValidatedNel[CommandError, KafkaClusterCommand] = {
      KafkaClusterCommand.UndeployTopology(
        command.metadata.withTimeStampUtcNow().withNewCorrelationId(),
        kafkaClusterId,
        topologyId,
        deploymentVersion,
        topologyVersion,
        topologiesStateForAuthorizationCode,
        deploymentOptions
      ).validNel
    }

    def kafkaClusterCommandsForDeploymentCommand(
      afterStateStores: AllStateEntities.ReadableVersionedStateStores,
      deploymentCommand: Command.DeploymentEntityCommand with Command.DeploymentIdCommand
    ): Seq[ValidatedNel[CommandError, KafkaClusterCommand]] =
      // deploymentCommand may be ReApply, Create or Update
      afterStateStores.deployment
        .getLatestLiveState(deploymentCommand.deploymentId)
        // if there is a deployment (after the command, which is the same as before for ReApply, but may be different for Create/Update)
        .map(d =>
          resolveDeploymentTopologyVersion(afterStateStores.topology, d.entity)
            .map(v =>
              // there is a proper topology version to be deployed -> deployTopologyCommand
              getDeployedTopology(
                afterStateStores,
                deploymentCommand.kafkaClusterId,
                deploymentCommand.topologyId,
                v,
                d.version
              )
              .map(_._2)
              .map(tp => {
                // calculating the topologies state from the current topology's namespace
                val topologiesStateForAuthorizationCode = getTopologiesStateOfNamespaceForAuthorizationCode(afterStateStores, tp.topology.namespace)
                deployTopologyCommand(topologiesStateForAuthorizationCode)(tp)
              })
              .toSeq
            )
            // there is no topology version -> undeployTopologyCommand
            .getOrElse(
              Seq(
                // we're un-deploying a topology (when it was already "Remove" in the deployment, we wouldn't be here)
                // don't know what version was deployed before, so just take the state based on the latest one, otherwise use the topologyId as the state
                undeployTopologyCommand(getTopologiesStateForAuthorizationCode(afterStateStores, deploymentCommand.topologyId))(
                  deploymentCommand.kafkaClusterId,
                  deploymentCommand.topologyId,
                  Some(d.version)
                )
              )
            )
        )
        // no deployment -> nothing to do
        .getOrElse(Seq.empty)

    val afterStateStores = beforeStateStores.withUncommittedChanges(changes)

    val beforeDeployedTopologies = getDeployedTopologies(beforeStateStores)
    val afterDeployedTopologies = getDeployedTopologies(afterStateStores)
    val (missingFromBefore, missingFromAfter, changesFromBeforeToAfter) =
      diffDeployedTopologies(beforeDeployedTopologies, afterDeployedTopologies)

    // the deploy-topology commands come from to-be deployed topologies missing from before and the changes (different topology version)
    val deployTopologyCommands = (missingFromBefore ++ changesFromBeforeToAfter.map(_._2))
      .map(td => deployTopologyCommand(getTopologiesStateOfNamespaceForAuthorizationCode(afterStateStores, td.topology.namespace))(td))
    // the un-deploy-topology commands come from the to-be deployed topologies missing from after
    val undeployTopologyCommands = missingFromAfter
      .map(td => undeployTopologyCommand(getTopologiesStateOfNamespaceForAuthorizationCode(afterStateStores, td.topology.namespace))(
        td.kafkaClusterId,
        td.topologyId,
        afterStateStores.deployment.getLatestLiveState(td.id).map(_.version),
        Some(td.topologyVersion)
      ))

    val kafkaClusterCommandsFromReApply = command match {
      // the re-apply deployment command has a special meaning: it generates further deploy/un-deploy topology commands
      // these shouldn't overlap with the previous ones, because currently a single re-apply command won't generate any entity changes
      case c: Command.DeploymentReApply =>
        kafkaClusterCommandsForDeploymentCommand(afterStateStores, c)

      // for DeploymentCreate/DeploymentUpdate commands, even if we didn't have any state-change and no deployTopologyCommands/undeployTopologyCommands
      // (meaning: no actual change in the set of deployed topology versions), we still send a deploy/un-deploy command
      // to make users' life easier. If there is nothing to do in the kafka-cluster, it won't do anything anyway, so no harm done.
      // this helps to resolve the common issue, when a deployment fails/half-succeeds, and being retried, and nothing happens (re-apply would be needed)
      case c: Command.DeploymentCreateOrUpdateCommand =>
        if (deployTopologyCommands.isEmpty && undeployTopologyCommands.isEmpty)
          kafkaClusterCommandsForDeploymentCommand(afterStateStores, c)
        else
          Seq.empty

      case _ => Seq.empty
    }

    // finally, if a deployment was deleted (it must have been remove=true already and the deployed topology must have been successfully removed completely),
    // we generate special un-deploy command that will remove the deployed-topology too
    val deleteDeploymentKafkaClusterCommands = changes.deployment
      .collect { case d: DeploymentStateChange.Deleted => beforeStateStores.deployment.getLatestLiveState(d.id) }.flatten
      .map(s => {
        // we're un-deploying a topology (when it was already "Remove" in the deployment)
        // don't know what version was deployed before, so just take the state based on the latest one, if there is one, otherwise use the topologyId as the state
        val topologiesStateForAuthorization = getTopologiesStateForAuthorizationCode(afterStateStores, s.entity.topologyId)
        undeployTopologyCommand(topologiesStateForAuthorization)(s.entity.kafkaClusterId, s.entity.topologyId, None)
      })
      // in theory this can't overlap with any other commands we just generated because deleting deployments
      // is possible only if the previous deployment was remove=true, in which case deployTopologyCommands and undeployTopologyCommands
      // can't contain this un-deploy (neither can kafkaClusterCommandsFromReApply from reapply commands)
      // even if there is overlap, it's not such a big deal, they will all be processed in this order by the kafka-cluster processor.

    // aggregating the possible errors
    val kafkaClusterCommandsOrError = (deployTopologyCommands ++ undeployTopologyCommands ++ kafkaClusterCommandsFromReApply ++ deleteDeploymentKafkaClusterCommands)
      .toVector
      .sequence[TopologyDeployable.MakeDeployableResult, KafkaClusterCommand]

    // deciding whether we succeeded or failed
    kafkaClusterCommandsOrError
      .toEither
      .map(a => KafkaClusterCommandFuncResult.success(kafkaClusterCommands = a))
      .leftMap(e => KafkaClusterCommandFuncResult.failure(command.failedResult(e.toList)))
      .merge
  }
}
