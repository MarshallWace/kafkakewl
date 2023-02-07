/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.common.deployedtopology.DeployedTopologyExtraSyntax
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessing.ProcessFuncResult
import com.mwam.kafkakewl.processor.kafkacluster.common.{KafkaAdminClientExtraSyntax, KafkaConnectionOperations, KafkaConsumerExtraSyntax}
import com.mwam.kafkakewl.processor.kafkacluster.deployment.{DeployTopology, DeployedTopologyMisc}
import com.mwam.kafkakewl.processor.kafkacluster.reset.ResetDeployedTopology
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContextExecutor

object KafkaClusterCommandProcessing {
  final case class Result(
    commandResult: KafkaClusterCommandResult,
    entityChanges: AllDeploymentEntitiesStateChanges = AllDeploymentEntitiesStateChanges()
  )

  type ProcessFuncResult = Either[KafkaClusterCommandResult.Failed, Result]
  object ProcessFuncResult {
    def success(
      commandResult: KafkaClusterCommandResult,
      entityChanges: AllDeploymentEntitiesStateChanges = AllDeploymentEntitiesStateChanges()): ProcessFuncResult =
      Right(Result(commandResult, entityChanges))

    def failure(failed: KafkaClusterCommandResult.Failed): ProcessFuncResult = Left(failed)
  }
}

private[kafkacluster] class KafkaClusterCommandProcessing(
  val kafkaClusterId: KafkaClusterEntityId,
  val connection: KafkaConnection,
  val kafkaClusterAdmin: KafkaClusterAdmin,
  val topicDefaults: TopicDefaults
)(implicit executionContext: ExecutionContextExecutor) extends DeployedTopologyExtraSyntax
  with DeployTopology
  with ResetDeployedTopology
  with KafkaConnectionOperations
  with KafkaConsumerExtraSyntax
  with KafkaAdminClientExtraSyntax
  with DeployedTopologyMisc
  with TopologyToDeployOperations
  with LazyLogging
  with MdcUtils
{
  def process(
    kafkaCluster: KafkaCluster,
    command: KafkaClusterCommand,
    stateStore: ReadableStateStore[DeployedTopology]
  ): ProcessFuncResult =
    command match {
      case c: KafkaClusterCommand.Diff => diff(kafkaCluster, c, stateStore)
      case c: KafkaClusterCommand.DeployTopology => deployTopology(kafkaCluster, c, stateStore)
      case c: KafkaClusterCommand.UndeployTopology => undeployTopology(kafkaCluster, c, stateStore)
      case c: KafkaClusterCommand.DeployedTopologyReset => reset(kafkaCluster, c, stateStore)
      case c: KafkaClusterCommand.DeployedTopologyTopicGetAll => getDeployedTopics(c, stateStore)
      case c: KafkaClusterCommand.DeployedTopologyTopicGet => getDeployedTopicDetails(c, stateStore)
      case c: KafkaClusterCommand.DeployedTopologyApplicationGetAll => getDeployedApplications(c, stateStore)
      case c: KafkaClusterCommand.DeployedTopologyApplicationGet => getDeployedApplicationDetails(c, stateStore)
    }
}
