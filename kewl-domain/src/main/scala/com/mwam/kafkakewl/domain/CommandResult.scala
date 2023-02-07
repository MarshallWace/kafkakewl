/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.kafka.{KafkaConsumerGroup, KafkaTopic}
import com.mwam.kafkakewl.domain.metrics.{ConsumerGroupMetrics, ConsumerGroupStatus, DeployedTopologyMetrics, DeployedTopologyMetricsCompact}
import com.mwam.kafkakewl.domain.topology._

import scala.collection.SortedMap
import scala.reflect.runtime.universe._

/**
  * The possible responses to commands.
  */
sealed trait CommandResponse
object CommandResponse {
  final case class None() extends CommandResponse

  // this trait helps us handle these respones in a non-generic way (without specify the entity type).
  // it's useful when e.g. want to filter the results with permissions
  sealed trait StateListBase {
    // this is needed for json encoding
    val entityType: Type
    val states: Seq[EntityState.Live[Entity]]
    def filter(f: EntityState.Live[Entity] => Boolean): CommandResponse
  }
  final case class StateList[E <: Entity : TypeTag](states: Seq[EntityState.Live[E]]) extends CommandResponse with StateListBase {
    val entityType: Type = typeOf[E]
    def filter(f: EntityState.Live[Entity] => Boolean): CommandResponse = StateList[E](states.filter(f))
  }

  // same reason
  sealed trait StateBase {
    // this is needed for json encoding
    val entityType: Type
    val state: EntityState.Live[Entity]
  }
  final case class State[E <: Entity : TypeTag](state: EntityState.Live[E]) extends CommandResponse with StateBase { val entityType: Type = typeOf[E] }

  final case class ResolvedTopologies(topologies: Topology.ResolvedTopologies) extends CommandResponse {
    def filter(f: (TopologyEntityId, Topology.ResolvedTopology) => Boolean): ResolvedTopologies =
      ResolvedTopologies(topologies.filter { case (id, resolvedTopology) => f(id, resolvedTopology) })
  }

  final case class ResolvedDeployedTopologies(deployedTopologies: TopologyToDeploy.ResolvedTopologies) extends CommandResponse {
    def filter(f: (TopologyEntityId, TopologyToDeploy.ResolvedTopology) => Boolean): ResolvedDeployedTopologies =
      ResolvedDeployedTopologies(deployedTopologies.filter { case (id, resolvedTopology) => f(id, resolvedTopology) })
  }

  // TODO TopicIds is PascalCase so that the response looks exactly the same as before when it came from the kafka-cluster processor
  final case class TopicIds(TopicIds: Seq[LocalTopicId]) extends CommandResponse
  // TODO ApplicationIds is PascalCase so that the response looks exactly the same as before when it came from the kafka-cluster processor
  final case class ApplicationIds(ApplicationIds: Seq[LocalApplicationId]) extends CommandResponse

  final case class DeployedTopic(
    topicId: LocalTopicId,
    topic: TopologyToDeploy.Topic,
    kafkaTopicInfo: Option[KafkaTopic.Info]
  ) extends CommandResponse

  final case class DeployedApplication(
    applicationId: LocalApplicationId,
    application: TopologyToDeploy.Application,
    consumerGroupInfo: Option[KafkaConsumerGroup.Info]
  ) extends CommandResponse

  final case class DeployedTopologiesMetricsResponse(
    deployedTopologiesMetrics: Seq[DeployedTopologyMetrics]
  ) extends CommandResponse

  final case class DeployedTopologiesMetricsCompactResponse(
    deployedTopologiesMetrics: Seq[DeployedTopologyMetricsCompact]
  ) extends CommandResponse

  final case class DeployedTopologyMetricsResponse(
    deployedTopologyMetrics: DeployedTopologyMetrics
  ) extends CommandResponse

  final case class DeployedTopologyMetricsCompactResponse(
    deployedTopologyMetrics: DeployedTopologyMetricsCompact
  ) extends CommandResponse

  object DeployedApplicationStatus {
    final case class ConsumerGroup(
      consumerGroup: String,
      status: ConsumerGroupStatus,
      topicPartitions: SortedMap[String, SortedMap[Int, ConsumerGroupMetrics]]
    )
  }

  final case class DeployedApplicationStatus(
    applicationId: LocalApplicationId,
    consumerGroup: Option[DeployedApplicationStatus.ConsumerGroup]
  ) extends CommandResponse

  final case class FromKafkaCluster(kafkaClusterResponse: KafkaClusterCommandResponse) extends CommandResponse
}

/**
  * The possible results of a command.
  */
sealed trait CommandResult extends CommandResultBase {
  def truncatedToString: String = {
    val wholeToString = this.toString
    if (wholeToString.length > 250) {
       wholeToString.take(250) + "..."
    } else {
      wholeToString
    }
  }
}
object CommandResult {
  final case class Failed(metadata: CommandMetadata, reasons: Seq[CommandError]) extends CommandResult with CommandResultBase.Failed

  final case class Succeeded(
    metadata: CommandMetadata,
    response: CommandResponse = CommandResponse.None(),
    kafkaClusterResults: Seq[KafkaClusterCommandResult] = Seq.empty,
    entityStateChanged: Boolean
  ) extends CommandResult
}
