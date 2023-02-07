/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import akka.actor.ActorSystem
import akka.stream.Materializer
import cats.syntax.either._
import com.mwam.kafkakewl.common.deployedtopology.DeployedTopologyExtraSyntax
import com.mwam.kafkakewl.common.metrics.MetricsServiceOps
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.common.{CommandProcessorExtensions, ReadableStateStore}
import com.mwam.kafkakewl.domain.Command.DeployedTopologyMetricsCommand
import com.mwam.kafkakewl.domain.EntityCompactors._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyToDeploy}
import com.mwam.kafkakewl.domain.{Command, CommandResponse, CommandResult, EntityState}

import scala.concurrent.ExecutionContextExecutor

trait DeployedTopologyStateReadOnlyCommandProcessing extends CommandProcessorExtensions
  with DeployedTopologyExtraSyntax
  with TopologyToDeployOperations {

  val topicDefaults: TopicDefaults

  private def deployedTopologyStateStore(
    command: Command.KafkaClusterDeployedTopologyEntityCommand,
    deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]
  ): ValueOrCommandError[ReadableStateStore[DeployedTopology]] =
    deployedTopologyStateStores.get(command.kafkaClusterId)
      .toRight(command.validationFailedResult(s"deployed-topology state-store for kafka cluster ${command.kafkaClusterId} does not exists"))

  private def getDeployedTopology(
    command: Command.KafkaClusterDeployedTopologyEntityCommand,
    stateStore: ReadableStateStore[DeployedTopology],
    topologyId: TopologyEntityId
  ): ValueOrCommandError[EntityState.Live[DeployedTopology]] =
    stateStore.getLatestLiveState(KafkaClusterAndTopology.id(command.kafkaClusterId, topologyId))
      .toRight(command.validationFailedResult(s"deployed-topology state-store for kafka cluster ${command.kafkaClusterId} does not contain topology $topologyId"))

  private def processMetricsCommand(
    command: Command.DeployedTopologyMetricsCommand with Command.StateReadOnlyCommand,
    metricsService: Option[MetricsServiceOps],
    deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]
  )(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer): ValueOrCommandError[CommandResult.Succeeded] = {
    // the current topologies by kafka-cluster id
    val allDeployedTopologies = deployedTopologyStateStores.values.flatMap(_.getLatestLiveStates).map(_.entity)

    command match {
      case c: Command.DeployedTopologyMetricsGetKafkaClustersAll =>

        for {
          deployedTopologiesMetrics <- metricsService match {
            case Some(ms) => ms.getDeployedTopologiesMetricsCommandResponse(allDeployedTopologies, allDeployedTopologies, c.compact).leftMap(c.failedResult)
            // no metrics service -> just return empty results (doesn't matter if it's compact or not, it'll empty)
            case None => CommandResponse.DeployedTopologiesMetricsResponse(Seq.empty).asRight
          }
        } yield c.succeededResult(deployedTopologiesMetrics)

      case c: Command.DeployedTopologyMetricsGetAll =>
        for {
          deployedTopologyStateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
          deployedTopologies = deployedTopologyStateStore.getLatestLiveStates.map(_.entity)
          deployedTopologiesMetrics <- metricsService match {
            case Some(ms) => ms.getDeployedTopologiesMetricsCommandResponse(allDeployedTopologies, deployedTopologies, c.compact).leftMap(c.failedResult)
            // no metrics service -> just return empty results (doesn't matter if it's compact or not, it'll empty)
            case None => CommandResponse.DeployedTopologiesMetricsResponse(Seq.empty).asRight
          }
        } yield c.succeededResult(deployedTopologiesMetrics)

      case c: Command.DeployedTopologyMetricsGet =>
        for {
          deployedTopologyStateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
          deployedTopology <- getDeployedTopology(c, deployedTopologyStateStore, c.topologyId).map(_.entity)
          deployedTopologyMetrics <- metricsService match {
            case Some(ms) => ms.getDeployedTopologyMetricsCommandResponse(allDeployedTopologies, deployedTopology, c.compact).leftMap(c.failedResult)
            // no metrics service -> just return empty results (doesn't matter if it's compact or not, it'll empty)
            case None => CommandResponse.DeployedTopologiesMetricsResponse(Seq.empty).asRight
          }
        } yield c.succeededResult(deployedTopologyMetrics)
    }
  }

  def process(
    command: Command.DeployedTopologyEntityCommand with Command.StateReadOnlyCommand,
    metricsService: Option[MetricsServiceOps],
    deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]
  )(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer): ValueOrCommandError[CommandResult.Succeeded] = {
    command match {
      case c: Command.DeployedTopologyGetKafkaClustersAll =>
        c.succeededResult(deployedTopologyStateStores.values.flatMap(_.getLatestLiveStates).toIndexedSeq, c.compact).asRight[CommandResult.Failed]

      case c: Command.DeployedTopologyGetAll =>
        for {
          deployedTopologyStateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
        } yield c.succeededResult(deployedTopologyStateStore.getLatestLiveStates, c.compact)

      case c: Command.DeployedTopologyGet =>
        for {
          deployedTopologyStateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
          deployedTopology <- getDeployedTopology(c, deployedTopologyStateStore, c.topologyId)
        } yield c.succeededResult(deployedTopology, c.compact)

      case c: Command.DeployedTopologiesGetResolved =>
        for {
          deployedTopologyStateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
        } yield {
          val currentTopologies = deployedTopologyStateStore.getLatestLiveStates
            .map(state => (state.entity.topologyId, state.entity.topologyWithVersion))
            .collect { case (tid, Some(topologyWithVersion)) => (tid, topologyWithVersion.topology) }
            .toMap
          val topologies = currentTopologies.filterKeys(tid => c.topologies.exists(_.doesMatch(tid)))

          val ops = new TopologyToDeployOperations() {}
          val allNodesMaps = ops.allNodesMapOfTopologies(currentTopologies.values)
          val topologiesRelationships = topologies.map {
            case (tid, t) => (tid, (t, ops.collectAllVisibleRelationshipsWithoutErrorsFor(tid, t, ops.resolveNodeRefFunc(allNodesMaps, t), topicDefaults).toSeq))
          }

          val resolvedTopologies = topologiesRelationships.map {
            case (topologyId, (topology, relationships))  =>
              (
                topologyId,
                TopologyToDeploy.ResolvedTopology(
                  topology,
                  relationships.map(_.toNodeIdNodeId)
                )
              )
          }

          command.succeededResult(CommandResponse.ResolvedDeployedTopologies(resolvedTopologies))
        }

      case c: Command.DeployedTopologyTopicGetAll =>
        for {
          stateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
          deployedTopology <- getDeployedTopology(c, stateStore, c.topologyId)
          topology <- deployedTopology.entity.topologyOrCommandErrors.leftMap(c.failedResult)
        } yield command.succeededResult(CommandResponse.TopicIds(topology.topics.keys.toSeq.sortBy(_.id)))

      case c: Command.DeployedTopologyApplicationGetAll =>
        for {
          stateStore <- deployedTopologyStateStore(c, deployedTopologyStateStores)
          deployedTopology <- getDeployedTopology(c, stateStore, c.topologyId)
          topology <- deployedTopology.entity.topologyOrCommandErrors.leftMap(c.failedResult)
        } yield command.succeededResult(CommandResponse.ApplicationIds(topology.applications.keys.toSeq.sortBy(_.id)))

      case c: DeployedTopologyMetricsCommand =>
        processMetricsCommand(c, metricsService, deployedTopologyStateStores)
    }
  }
}
