/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.common.ReadableStateStore
import com.mwam.kafkakewl.common.deployedtopology.DeployedTopologyExtraSyntax
import com.mwam.kafkakewl.common.topology.TopologyToDeployOperations
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaClusterAndTopology, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.{KafkaClusterCommand, KafkaClusterCommandResponse}
import com.mwam.kafkakewl.processor.kafkacluster.{KafkaClusterAdmin, KafkaClusterCommandProcessing}
import com.typesafe.scalalogging.Logger

private[kafkacluster] trait DeployedTopologyMisc {
  this: DeployedTopologyExtraSyntax with TopologyToDeployOperations =>

  val kafkaClusterId: KafkaClusterEntityId
  protected val logger: Logger
  val kafkaClusterAdmin: KafkaClusterAdmin
  val topicDefaults: TopicDefaults

  def getDeployedTopics(
    command: KafkaClusterCommand.DeployedTopologyTopicGetAll,
    stateStore: ReadableStateStore[DeployedTopology]
  ): KafkaClusterCommandProcessing.ProcessFuncResult = {
    val deployedTopologyId = KafkaClusterAndTopology.id(command.kafkaClusterId, command.topologyId)

    val result = for {
      deployedTopology <- stateStore.getLatestOrCommandErrors(deployedTopologyId)
      topology <- deployedTopology.topologyOrCommandErrors
    } yield KafkaClusterCommandResponse.TopicIds(topology.topics.keys.toSeq.sortBy(_.id))

    result
      .map(r => KafkaClusterCommandProcessing.Result(command.succeededResult(r)))
      .left.map(command.failedResult)
  }

  def getDeployedTopicDetails(
    command: KafkaClusterCommand.DeployedTopologyTopicGet,
    stateStore: ReadableStateStore[DeployedTopology]
  ): KafkaClusterCommandProcessing.ProcessFuncResult = {

    val deployedTopologyId = KafkaClusterAndTopology.id(command.kafkaClusterId, command.topologyId)

    val result = for {
      deployedTopology <- stateStore.getLatestOrCommandErrors(deployedTopologyId)
      topology <- deployedTopology.topologyOrCommandErrors
      topic <- topology.topicOrCommandErrors(topology.fullyQualifiedTopicId(command.topicId))
      topicStats <- kafkaClusterAdmin.getTopicInfo(topic.name)
    } yield KafkaClusterCommandResponse.DeployedTopic(command.topicId, topic, topicStats)

    result
      .map(r => KafkaClusterCommandProcessing.Result(command.succeededResult(r)))
      .left.map(command.failedResult)
  }

  def getDeployedApplications(
    command: KafkaClusterCommand.DeployedTopologyApplicationGetAll,
    stateStore: ReadableStateStore[DeployedTopology]
  ): KafkaClusterCommandProcessing.ProcessFuncResult = {
    val deployedTopologyId = KafkaClusterAndTopology.id(command.kafkaClusterId, command.topologyId)

    val result = for {
      deployedTopology <- stateStore.getLatestOrCommandErrors(deployedTopologyId)
      topology <- deployedTopology.topologyOrCommandErrors
    } yield KafkaClusterCommandResponse.ApplicationIds(topology.applications.keys.toSeq.sortBy(_.id))

    result
      .map(r => KafkaClusterCommandProcessing.Result(command.succeededResult(r)))
      .left.map(command.failedResult)
  }

  def getDeployedApplicationDetails(
    command: KafkaClusterCommand.DeployedTopologyApplicationGet,
    stateStore: ReadableStateStore[DeployedTopology]
  ): KafkaClusterCommandProcessing.ProcessFuncResult = {

    val deployedTopologyId = KafkaClusterAndTopology.id(command.kafkaClusterId, command.topologyId)
    val currentTopologies = stateStore.currentTopologies()

    val result = for {
      deployedTopology <- stateStore.getLatestOrCommandErrors(deployedTopologyId)
      topology <- deployedTopology.topologyOrCommandErrors

      applicationId = topology.fullyQualifiedApplicationId(command.applicationId)

      application <- topology.applicationOrCommandErrors(applicationId)
      relationships = allRelationshipsOfApplication(currentTopologies, deployedTopology.topologyId, topology, applicationId, topicDefaults)

      // mapping from topic-name to topic-id, including external topics: so that the topic names for which the group
      // has committed offsets for can be mapped back to topic-ids
      consumedTopicIdsByName = relationships
        .filter(_.relationship.isConsume)
        .map(r => (r.resolveNode2.topologyNode.node.name, r.resolveNode2.topologyNode.nodeId.id))
        .toMap

      consumerGroupStats <- application.actualConsumerGroup
        // there is consumer group for the application
        .map { consumerGroupId =>
          kafkaClusterAdmin.getConsumerGroupInfo(consumerGroupId)
            .map(i =>
              i.copy(topics =
                i.topics
                  // filtering for topics with consume-relationship to this app, as well as mapping back to the topic-id
                  .flatMap { case (topicName, topicInfo) => consumedTopicIdsByName.get(topicName).map((_, topicInfo)) }
              )
            )
            .map(Some(_))
        }
        // no consumer group -> it's fine, it'll be None
        .getOrElse(Right(None))
    } yield KafkaClusterCommandResponse.DeployedApplication(command.applicationId, application, consumerGroupStats)

    result
      .map(r => KafkaClusterCommandProcessing.Result(command.succeededResult(r)))
      .left.map(command.failedResult)
  }
}
