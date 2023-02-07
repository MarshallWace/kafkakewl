/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.common.deployedtopology.DeployedTopologyExtraSyntax
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetApplicationOptions
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessing
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessing.ProcessFuncResult
import com.mwam.kafkakewl.processor.kafkacluster.common.{KafkaAdminClientExtraSyntax, KafkaConnectionOperations, KafkaConsumerExtraSyntax}

import scala.concurrent.ExecutionContextExecutor

private[kafkacluster] trait ResetDeployedTopology
  extends ResetCommon
    with ResetApplicationOffsets
    with ResetConnector {

  this: DeployedTopologyExtraSyntax
    with KafkaConnectionOperations
    with KafkaConsumerExtraSyntax
    with KafkaAdminClientExtraSyntax
    with TopologyToDeployOperations =>

  val kafkaClusterId: KafkaClusterEntityId

  /**
    * Executes the specified reset deployed-topology command.
    *
    * @param kafkaCluster the kafka cluster
    * @param command the reset deployed-topology command
    * @param stateStore the deployed-topology state-store
    * @return the success or failure result with all the details
    */
  def reset(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand.DeployedTopologyReset,
    stateStore: ReadableStateStore[DeployedTopology]
  )(implicit executionContext: ExecutionContextExecutor): ProcessFuncResult = {

    val currentTopologies = stateStore.getLatestLiveStates
      .flatMap(s => s.entity.topologyWithVersion.map(pv => (s.entity.topologyId, pv.topology)))
      .toMap
    val deployedTopologyId = KafkaClusterAndTopology.id(command.kafkaClusterId, command.topologyId)

    val result = for {
      deployedTopology <- stateStore.getLatestOrCommandErrors(deployedTopologyId).left.map(command.failedResult)
      topologyAndVersion <- deployedTopology.topologyWithVersion.toRight(command.failedResult(CommandError.validationError(s"deployed topology '$deployedTopologyId' does not contain any topology")))
      topology = topologyAndVersion.topology
      applicationId = topology.fullyQualifiedApplicationId(command.options.application)
      result <- command.options.options match {
        case o: ResetApplicationOptions.ApplicationTopicOffsetsOptions =>
          resetApplicationOffsets(kafkaCluster, command, currentTopologies, deployedTopology.topologyId, topology, applicationId, o)
        case o: ResetApplicationOptions.Connector =>
          resetConnectorApplication(kafkaCluster, command, topology, applicationId, o)
        case o: ResetApplicationOptions.ConnectReplicator =>
          resetConnectReplicatorApplication(kafkaCluster, command, topology, applicationId, o)
      }
    } yield result

    result.map(r => KafkaClusterCommandProcessing.Result(r))
  }
}
