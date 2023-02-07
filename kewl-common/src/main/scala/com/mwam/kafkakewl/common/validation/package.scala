/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import cats.data.Validated.{Invalid, Valid}
import com.mwam.kafkakewl.domain.kafkacluster.KafkaCluster
import com.mwam.kafkakewl.domain.topology.TopologyEntityId
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.utils.{ApplicationMetrics, DurationExtensions, durationOf, failFast}
import com.typesafe.scalalogging.Logger

import scala.concurrent.duration.FiniteDuration

package object validation {
  private def logValidationErrorsAndFailFast(logger: Logger, validationDuration: FiniteDuration, validationResult: Validation.Result): Unit = {
    validationResult match {
      case Invalid(validationErrors) =>
        for (validationError <- validationErrors.toList) {
          logger.error(validationError.toString)
        }
        ApplicationMetrics.errorCounter.inc()
        logger.error(s"TERMINATING due to invalid state")
        failFast()

      case Valid(_) =>
        logger.info(f"the state-store is valid (validation took ${validationDuration.toMillisDouble}%.3f ms)")
    }
  }

  /**
   * Validates the specified state store to check whether it's in a valid state with itself or not.
   *
   * In fact currently it validates only the topologies.
   *
   * The purpose of this is that some config changes can cause invalid topologies, e.g. changing the topic-default other consumer/producer namespaces, so
   * it's safest if we validate all topologies at startup to make sure config is correct.
   *
   * @param logger the logger
   * @param inMemoryStateStores the in-memory state store
   * @param topicDefaults the topic defaults to use for the validation
   */
  def validateStateStore(
    logger: Logger,
    inMemoryStateStores: AllStateEntities.InMemoryVersionedStateStores,
    topicDefaults: TopicDefaults
  ): Unit = {
    val currentTopologies = inMemoryStateStores.topology.getLatestLiveStates.map(s => (TopologyEntityId(s.id), s.entity)).toMap
    val (validationResult, validationDuration) = durationOf {
      TopologiesValidator.validateAllTopologies(currentTopologies, topicDefaults)
    }

    logValidationErrorsAndFailFast(logger, validationDuration, validationResult)
  }

  /**
   * Validates the specified deployment state store to check whether it's in a valid state with itself or not.
   *
   * In fact currently it validates only the deployed topologies.
   *
   * The purpose of this is that some config changes can cause invalid topologies, e.g. changing the topic-default other consumer/producer namespaces, so
   * it's safest if we validate all topologies at startup to make sure config is correct.
   *
   * @param logger the logger
   * @param inMemoryStateStores the in-memory deployment state store
   * @param topicDefaults the topic defaults to use for the validation
   */
  def validateDeploymentStateStore(
    logger: Logger,
    inMemoryStateStores: AllDeploymentEntities.InMemoryStateStores,
    kafkaCluster: KafkaCluster,
    topicDefaults: TopicDefaults
  ): Unit = {
    val currentDeployedTopologies = inMemoryStateStores.deployedTopology.getLatestLiveStates.map(dp => (dp.entity.topologyId, dp)).toMap
    val currentTopologies = currentDeployedTopologies
      .mapValues(_.entity.topologyWithVersion)
      .collect { case (p, Some(pv)) => (p, pv.topology) }

    val (validationResult, validationDuration) = durationOf {
      TopologiesToDeployValidator.validateAllTopologies(currentTopologies, kafkaCluster.kafkaCluster, kafkaCluster, topicDefaults)
    }

    logValidationErrorsAndFailFast(logger, validationDuration, validationResult)
  }
}
