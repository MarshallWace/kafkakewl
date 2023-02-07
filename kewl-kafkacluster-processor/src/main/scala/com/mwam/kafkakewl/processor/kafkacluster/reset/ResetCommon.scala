/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.reset

import com.mwam.kafkakewl.common.AuthorizationCode
import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.common.validation.Validation.Result
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.kafkacluster.KafkaCluster
import com.mwam.kafkakewl.domain.topology.{ApplicationId, TopologyToDeploy}

trait ResetCommon {
  def validateApplication(topologyToDeploy: TopologyToDeploy, application: ApplicationId): Result =
    Validation.Result.validationErrorIf(
      !topologyToDeploy.fullyQualifiedApplications.contains(application),
      s"application '$application' does not exist in the topology"
    )

  def failIfAnyErrors(errors: Seq[CommandError]): ValueOrCommandErrors[Unit] =
    Either.cond(errors.isEmpty, (), errors)

  def failIfAuthorizationCodeNeeded(
    kafkaCluster: KafkaCluster,
    topologyToDeployWithAuthorizationCode: Option[Boolean],
    resetSummary: String,
    command: KafkaClusterCommand,
    hasChanges: Boolean,
    authorizationCode: Option[String]
  ): ValueOrCommandErrors[Unit] = {
    val validLastMinutes = 2

    val authorizationCodeInput = AuthorizationCode.Generator.inputFor(kafkaCluster.kafkaCluster, command.userName, resetSummary)
    def isAuthorizationCodeValid(code: String) = AuthorizationCode.isValidInLastMinutes(code, validLastMinutes, authorizationCodeInput)

    Either.cond(
      !topologyToDeployWithAuthorizationCode.getOrElse(kafkaCluster.requiresAuthorizationCode) || command.dryRun || !hasChanges || authorizationCode.exists(isAuthorizationCodeValid),
      (),
      {
        val currentCode = AuthorizationCode.generateCurrent(authorizationCodeInput)
        Seq(CommandError.permissionError(
          if (authorizationCode.nonEmpty) s"authorization code invalid, new authorization code: $currentCode"
          else s"authorization code required: $currentCode"
        ))
      }
    )
  }
}
