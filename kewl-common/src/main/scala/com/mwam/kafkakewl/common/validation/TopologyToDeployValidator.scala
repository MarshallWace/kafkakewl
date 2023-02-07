/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import cats.syntax.option._
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId, NonKewlKafkaResources}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyToDeploy}
import com.mwam.kafkakewl.utils._

object TopologyToDeployValidator {
  def ensureTopicNamesAreNotNonKewl(kafkaClusterId: KafkaClusterEntityId, nonKewlKafkaResources: NonKewlKafkaResources, newTopologyToDeploy: TopologyToDeploy): Validation.Result = {
    val nonKewlTopics = newTopologyToDeploy.fullyQualifiedTopics.values.filter(t => nonKewlKafkaResources.isTopicNonKewl(t.name))
    if (nonKewlTopics.nonEmpty)
      Validation.Result.validationError(s"Topics ${nonKewlTopics.map(_.name.quote).mkString(", ")} are non-kewl in the kafka cluster '$kafkaClusterId'")
    else
      Validation.Result.success
  }

  def ensureGroupNamesAreNotNonKewl(kafkaClusterId: KafkaClusterEntityId, nonKewlKafkaResources: NonKewlKafkaResources, newTopologyToDeploy: TopologyToDeploy): Validation.Result = {
    val nonKewlApplications = newTopologyToDeploy.fullyQualifiedApplications
      .mapValues(_.actualConsumerGroup)
      .collect { case (id, Some(cg)) if nonKewlKafkaResources.isGrougNonKewl(cg) => (id, cg) }
    if (nonKewlApplications.nonEmpty) {
      val applicationsAsString = nonKewlApplications.map { case (id, cg) => s"${id.quote}/${cg.quote}" }.mkString(", ")
      Validation.Result.validationError(s"Applications' $applicationsAsString are non-kewl in the kafka cluster '$kafkaClusterId'")
    } else {
      Validation.Result.success
    }
  }

  def validateTopologyTopics(kafkaCluster: KafkaCluster, newTopologyToDeploy: TopologyToDeploy): Validation.Result = {
    val possibleReplicaPlacementIds = kafkaCluster.replicaPlacementConfigs.keys.map(_.quote).toSeq.sorted.mkString(", ")

    newTopologyToDeploy.topics
      .map { case (_, topic) =>
        val topicReplicaPlacementExistsValid = topic.replicaPlacement match {
          case Some(replicaPlacement) => Validation.Result.validationErrorIf(
            !kafkaCluster.replicaPlacementConfigs.contains(replicaPlacement),
            if (kafkaCluster.replicaPlacementConfigs.nonEmpty) {
              s"'replicaPlacement' = ${replicaPlacement.quote} is invalid: possible pre-defined options: $possibleReplicaPlacementIds"
            } else {
              s"'replicaPlacement' = ${replicaPlacement.quote} is invalid: you should ask the administrators to setup pre-defined replica-placements for the kafka-cluster"
            }
          )
          case None => Validation.Result.success
        }

        val topicReplicationFactorValid = topic.replicationFactor match {
          case Some(replicationFactor) => Validation.Result.validationErrorIf(replicationFactor <= 0, s"if 'replicationFactor' is set it must be positive but '$replicationFactor' is not")
          case None => Validation.Result.success
        }

        val notAllowedTopicConfigKeysResult = topic.config.keys
          .filter(topicConfigKey => !kafkaCluster.isTopicConfigKeyAllowedInTopologies(topicConfigKey))
          .map { topicConfigKey =>
            val replicaPlacementIdsWithTheseTopicConfigs = kafkaCluster.replicaPlacementConfigs
              .collect {
                case (replicaPlacementId, replicaPlacementConfig) if replicaPlacementConfig.get(topicConfigKey).exists(_.default.nonEmpty) => replicaPlacementId.quote
              }
              .mkString(", ")
            if (replicaPlacementIdsWithTheseTopicConfigs.nonEmpty)
              Validation.Result.validationError(s"The ${topicConfigKey.quote} topic config is not allowed by the administrators. If you really need to use this topic-config try setting the 'replicaPlacement' to one of the following: $replicaPlacementIdsWithTheseTopicConfigs")
            else
              Validation.Result.validationError(s"The ${topicConfigKey.quote} topic config is not allowed by the administrators. If you really need to use this topic-config ask the administrators to allow it in the kafka-cluster ${kafkaCluster.kafkaCluster.quote}")
          }
          .combine()

        // checking the replica-placement
        val topicReplicaPlacementId = topic.replicaPlacement.orElse(kafkaCluster.defaultReplicaPlacementId)
        val topicReplacementConfig = topicReplicaPlacementId.flatMap(kafkaCluster.replicaPlacementConfigs.get)
        val topicReplicaPlacementValid = (topicReplicaPlacementId, topicReplacementConfig) match {
          case (Some(replicaPlacementId), Some(replicaPlacementConfig)) =>
            val replicaPlacementOverrideResult = replicaPlacementConfig
              .collect { case (topicConfigKey, topicConfigDefault) if !topicConfigDefault.overridable && topic.config.contains(topicConfigKey) =>
                topicConfigDefault.default match {
                  case Some(default) =>
                    if (default == topic.config(topicConfigKey)) {
                      Validation.Result.validationError(
                        s"replicaPlacement = ${replicaPlacementId.quote} does not allow to set the ${topicConfigKey.quote} topic config. You can remove this topic config from your topic because the replica-placement's default is the same as what you just set: '$default'"
                      )
                    } else {
                      Validation.Result.validationError(
                        s"replicaPlacement = ${replicaPlacementId.quote} does not allow to set the ${topicConfigKey.quote} topic config. You can remove this topic config from your topic if you're happy with the replica-placement's default: '$default', set 'replicaPlacement' to another one that allows this config or sets it to value you need. Available replica-placements: $possibleReplicaPlacementIds"
                      )
                    }
                  case None =>
                    Validation.Result.validationError(
                      s"replicaPlacement = ${replicaPlacementId.quote} does not allow to set the ${topicConfigKey.quote} topic config. You can remove this topic config from your topic if you don't need it, set 'replicaPlacement' to another one that allows this config or sets it to value you need. Available replica-placements: $possibleReplicaPlacementIds"
                    )
                }
              }
              .combine()

            val topicReplicaPlacementReplicationFactorResult = if (kafkaCluster.resolveTopicConfig(replicaPlacementId.some, topic.config).contains(TopicConfigKeys.confluentPlacementConstraints)) {
              // the actual topic config resolved from the replica-placement contains "confluent.placement.constraints" -> we can't tolerate a replication-factor

              topic.replicationFactor match {
                case Some(_) =>
                  // the replica-placement ids where the replica-placement is empty (i.e. it doesn't set the "confluent.placement.constraints" config or sets it explicitly to none)
                  val nullReplicaPlacementIds = kafkaCluster.replicaPlacementConfigs
                    .collect { case (replicaPlacementId, replicaPlacementConfig) if replicaPlacementConfig.get(TopicConfigKeys.confluentPlacementConstraints).forall(_.default.isEmpty) => replicaPlacementId.quote }
                    .mkString(", ")
                  if (nullReplicaPlacementIds.nonEmpty) {
                    Validation.Result.validationError(s"'replicationFactor' must not be set if there is a non-empty 'replicaPlacement' (= ${replicaPlacementId.quote}). Either remove the 'replicationFactor' property from topic ${topic.name.quote} or set the 'replicaPlacement' to one of the following: $nullReplicaPlacementIds")
                  } else {
                    Validation.Result.validationError(s"'replicationFactor' must not be set if there is a non-empty 'replicaPlacement' (= ${replicaPlacementId.quote}). Remove the 'replicationFactor' property from topic ${topic.name.quote}")
                  }

                case None => Validation.Result.success
              }
            } else {
              Validation.Result.success
            }

            replicaPlacementOverrideResult ++ topicReplicaPlacementReplicationFactorResult
          case _ =>
            // we report success if the replicaPlacementId doesn't exist in the kafka-cluster (other validation will spot that) OR if it exists but it's an "empty" one (so in reality, there is no replica-placement)
            Validation.Result.success
        }

        Seq(topicReplicaPlacementExistsValid, topicReplicationFactorValid, notAllowedTopicConfigKeysResult, topicReplicaPlacementValid).combine()
      }
      .combine()
  }

  def validateStandaloneTopology(
    newTopologyId: TopologyEntityId,
    newTopologyOrNone: Option[TopologyToDeploy],
    kafkaClusterId: KafkaClusterEntityId,
    kafkaCluster: KafkaCluster
  ): Validation.Result = {
    Seq(
      newTopologyOrNone.map(TopologyToDeployValidatorStandalone.validateStandaloneTopology(newTopologyId, _)),
      newTopologyOrNone.map(ensureTopicNamesAreNotNonKewl(kafkaClusterId, kafkaCluster.nonKewl, _)),
      newTopologyOrNone.map(ensureGroupNamesAreNotNonKewl(kafkaClusterId, kafkaCluster.nonKewl, _)),
      newTopologyOrNone.map(validateTopologyTopics(kafkaCluster, _))
    ).flatten.combine()
  }

  def validateTopology(
    currentTopologiesMap: Map[TopologyEntityId, TopologyToDeploy],
    newTopologyId: TopologyEntityId,
    newTopologyOrNone: Option[TopologyToDeploy],
    kafkaClusterId: KafkaClusterEntityId,
    kafkaCluster: KafkaCluster,
    topicDefaults: TopicDefaults
  ): Validation.Result = {
    Seq(
      validateStandaloneTopology(newTopologyId, newTopologyOrNone, kafkaClusterId, kafkaCluster),
      TopologyToDeployValidatorWithOthers.validateTopologyWithOthers(TopologyValidator.allowedCustomRelationships, currentTopologiesMap, newTopologyId, newTopologyOrNone, topicDefaults),
    ).combine()
  }
}
