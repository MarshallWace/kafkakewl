/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import cats.syntax.either._
import com.mwam.kafkakewl.kafka.utils.{KafkaAdminConfig, KafkaConfigProperties, KafkaConnection, KafkaConnectionInfo}
import com.mwam.kafkakewl.domain.{CommandError, DeploymentEnvironments}
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.utils._
import org.apache.kafka.clients.admin.{AdminClient, ListTopicsOptions}

import scala.util.Try

object KafkaClusterValidator extends ValidationUtils {
  def validateKafkaClusterConnection(jaasConfig: Option[String], kafkaCluster: KafkaCluster): Validation.Result = {
    val kafkaAdminConfig = KafkaAdminConfig(KafkaConnection(KafkaConnectionInfo(kafkaCluster.brokers, kafkaCluster.securityProtocol, kafkaCluster.kafkaClientConfig), jaasConfig))
    val kafkaClusterConnectionResult = for {
      adminClient <- Try(AdminClient.create(KafkaConfigProperties.forAdmin(kafkaAdminConfig)))
      // don't really care about topic-names, only whether it fails or succeeds
      _ <- {
        val topicNames = adminClient.listTopics(new ListTopicsOptions().timeoutMs(10000)).names().toTry
        // this is important, otherwise the admin client may keep trying to connect forever
        adminClient.close()
        topicNames
      }
    } yield ()

    kafkaClusterConnectionResult
      .toEither
      .leftMap(t => CommandError.validationError(s"kafka-cluster connection '${kafkaCluster.toShortString}' invalid, couldn't get the list of topics: ${t.toErrorMessage}"))
      .toValidatedNel
  }

  def ensureKafkaClusterBrokersAreUnique(
    currentTopologiesMap: Map[KafkaClusterEntityId, KafkaCluster],
    newKafkaClusterId: KafkaClusterEntityId,
    newKafkaCluster: KafkaCluster
  ): Result = {
    (currentTopologiesMap - newKafkaClusterId)
      .find { case (_, kc) => kc.brokers == newKafkaCluster.brokers }.map(_._1)
      .map(kafkaClusterId => Validation.Result.validationError(s"kafka-cluster brokers '${newKafkaCluster.brokers}' is already used in another kafka-cluster '$kafkaClusterId'"))
      .getOrElse(Validation.Result.success)
  }

  def validateKafkaCluster(newKafkaClusterId: KafkaClusterEntityId, newKafkaCluster: KafkaCluster): Validation.Result = {
    val brokersValidationResult = newKafkaCluster.brokers.validateNonEmpty("kafka-cluster brokers list")
    val duplicateDeploymentEnvironments = newKafkaCluster.environments.map(_._1).duplicates
    val firstDeploymentEnvironmentIdOrNone = newKafkaCluster.environments.headOption.map(_._1)

    brokersValidationResult ++
      Validation.Result.validationErrorIf(newKafkaCluster.kafkaCluster.isEmpty, s"kafka-cluster id of ${newKafkaClusterId.quote} cannot be empty") ++
      Validation.Result.validationErrorIf(duplicateDeploymentEnvironments.nonEmpty, s"duplicate deployment environments: ${duplicateDeploymentEnvironments.map(_.quote).mkString(", ")}") ++
      Validation.Result.validationErrorIf(newKafkaCluster.environments.isEmpty, s"expecting at least one ('${DeploymentEnvironments.default}') deployment environment") ++
      Validation.Result.validationErrorIf(firstDeploymentEnvironmentIdOrNone.exists(_ != DeploymentEnvironments.default), s"expecting the first deployment environment to be '${DeploymentEnvironments.default}' but was ${firstDeploymentEnvironmentIdOrNone.map(_.quote).getOrElse("")}") ++
      Validation.Result.validationErrorIf(newKafkaCluster.replicaPlacementConfigs.isEmpty && newKafkaCluster.defaultReplicaPlacementId.nonEmpty, s"the defaultReplicaPlacementId must not be set if the replicaPlacements is empty") ++
      Validation.Result.validationErrorIf(newKafkaCluster.replicaPlacementConfigs.nonEmpty && newKafkaCluster.defaultReplicaPlacementId.isEmpty, s"the defaultReplicaPlacementId must be set if the replicaPlacements is not empty") ++
      Validation.Result.validationErrorIf(newKafkaCluster.replicaPlacementConfigs.nonEmpty && newKafkaCluster.defaultReplicaPlacementId.exists(rp => !newKafkaCluster.replicaPlacementConfigs.contains(rp)), s"the replicaPlacements doesn't have the defaultReplicaPlacementId = ${newKafkaCluster.defaultReplicaPlacementId.map(_.quote).getOrElse("")}") ++
      Validation.Result.validationErrorIf(newKafkaCluster.systemTopicsReplicaPlacementId.exists(rp => !newKafkaCluster.replicaPlacementConfigs.contains(rp)), s"the replicaPlacements doesn't have the systemTopicsReplicaPlacementId = ${newKafkaCluster.systemTopicsReplicaPlacementId.map(_.quote).getOrElse("")}") ++
      newKafkaCluster.replicaPlacementConfigs.collect { case (id, replicaPlacementConfig) if replicaPlacementConfig.contains("") => Validation.Result.validationError(s"replicaPlacementConfig ${id.quote} contains empty topic config key") }.combine()
  }

  def validateKafkaCluster(
    jaasConfig: Option[String],
    newKafkaClusterId: KafkaClusterEntityId,
    newKafkaCluster: KafkaCluster
  ): Validation.Result = validateKafkaCluster(newKafkaClusterId, newKafkaCluster) ++ validateKafkaClusterConnection(jaasConfig, newKafkaCluster)

  def validateKafkaClusterWithOthers(
    currentTopologiesMap: Map[KafkaClusterEntityId, KafkaCluster],
    newKafkaClusterId: KafkaClusterEntityId,
    newKafkaClusterOrNone: Option[KafkaCluster]
  ): Validation.Result = {
    newKafkaClusterOrNone
      .map(ensureKafkaClusterBrokersAreUnique(currentTopologiesMap, newKafkaClusterId, _))
      .getOrElse(Validation.Result.success)
  }
}
