/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

/**
  * A message in the change-log store. It contains exactly one of state and kafkaclusterId/deployment.
  *
  * @param state the optional state transaction item
  * @param kafkaClusterId the optional kafka cluster id, if deployment is not empty
  * @param deployment the optional deployment transaction item
  */
final case class ChangeLogMessage(
  state: Option[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]] = None,
  kafkaClusterId: Option[KafkaClusterEntityId] = None,
  deployment: Option[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]] = None
) {
  def transactionId(): String =
    (state.map(_.transactionId) orElse deployment.map(_.transactionId))
      .getOrElse(throw new RuntimeException("At least one of state or deployment must be non-empty"))
}
