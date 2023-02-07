/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.{AllStateEntities, ReadableStateStore}
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, Deployment, DeploymentEntityId, DeploymentTopologyVersion}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.domain.{Command, CommandError}

object CommandProcessorValidator {
  def validateDeploymentDelete(
    ss: ReadableStateStore[Deployment],
    dss: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]],
    deploymentId: DeploymentEntityId
  ): Validation.Result = {
    ss.getLatestLiveState(deploymentId)
      .map(s => {
        val result = for {
          deployedTopologyStateStore <- dss.get(s.entity.kafkaClusterId).toRight(CommandError.otherError(s"should not happen: no deployed-topology entities for kafka-cluster '${s.entity.kafkaClusterId}'"))
          deployedTopology <- deployedTopologyStateStore.getLatestLiveState(deploymentId).toRight(CommandError.otherError(s"should not happen: no deployed-topology for deployment '${s.id}' while it still exists."))
        } yield {
          if (!s.entity.topologyVersion.isInstanceOf[DeploymentTopologyVersion.Remove]) {
            Validation.Result.validationError(s"cannot delete deployment if it's topology version (${s.entity.topologyVersion}) is NOT Remove")
          } else {
            Validation.Result.validationErrorIf(
              // This is very unlikely to happen: it means, the deployment is to be REMOVE-ed already, but the kafka-cluster processor hasn't processed it yet, and hasn't
              // recorded it in the DeployedTopology. We should be in this state only for a short amount of time, definitely not forever.
              deployedTopology.entity.topologyWithVersion.nonEmpty || deployedTopology.entity.deploymentVersion != s.version,
              s"cannot delete a REMOVE-ed deployment (version=${s.version}) if it hasn't been fully processed " +
                s"(last deployed-topology's deployment version=${deployedTopology.entity.deploymentVersion}, successful=${deployedTopology.entity.allActionsSuccessful})"
            )
          }
        }
        result.left.map(Validation.Result.error).merge
      })
      // if there is no deployment, we accept it, the later validations will check it anyway,
      // but no need to care here
      .getOrElse(Validation.Result.success)
  }

  def validateCommand(
    command: Command,
    ss: AllStateEntities.ReadableVersionedStateStores,
    dss: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]
  ): Validation.Result =
    command match {
      case c: Command.DeploymentDelete =>
        validateDeploymentDelete(ss.deployment, dss, c.deploymentId)

      case _ => Validation.Result.success
    }
}
