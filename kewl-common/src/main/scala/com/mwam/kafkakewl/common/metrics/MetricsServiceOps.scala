/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

import cats.syntax.either._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.{CommandError, CommandResponse, ValueOrCommandError}
import com.mwam.kafkakewl.domain.metrics.{DeployedTopologyMetrics, DeployedTopologyMetricsCompact}

trait MetricsServiceOps {
  this: MetricsService =>

  def getDeployedTopologyMetrics(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopology: DeployedTopology
  ): ValueOrCommandError[DeployedTopologyMetrics] = for {
    dtms <- getDeployedTopologiesMetrics(allCurrentDeployedTopologies, deployedTopology :: Nil)
    dtm <- dtms.find(_.id == deployedTopology.id)
      .map(_.asRight)
      .getOrElse(CommandError.otherError(s"no metrics for ${deployedTopology.id}").asLeft)
  } yield dtm

  def getDeployedTopologyMetricsCompact(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopology: DeployedTopology
  ): ValueOrCommandError[DeployedTopologyMetricsCompact] = for {
    dtms <- getDeployedTopologiesMetricsCompact(allCurrentDeployedTopologies, deployedTopology :: Nil)
    dtm <- dtms.find(_.id == deployedTopology.id)
      .map(_.asRight)
      .getOrElse(CommandError.otherError(s"no metrics for ${deployedTopology.id}").asLeft)
  } yield dtm


  def getDeployedTopologiesMetricsCommandResponse(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopologies: Iterable[DeployedTopology],
    compact: Boolean
  ): ValueOrCommandError[CommandResponse] = {
    if (compact)
      getDeployedTopologiesMetricsCompact(allCurrentDeployedTopologies, deployedTopologies).map(CommandResponse.DeployedTopologiesMetricsCompactResponse)
    else
      getDeployedTopologiesMetrics(allCurrentDeployedTopologies, deployedTopologies).map(CommandResponse.DeployedTopologiesMetricsResponse)
  }

  def getDeployedTopologyMetricsCommandResponse(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopology: DeployedTopology,
    compact: Boolean
  ): ValueOrCommandError[CommandResponse] = {
    if (compact)
      getDeployedTopologyMetricsCompact(allCurrentDeployedTopologies, deployedTopology).map(CommandResponse.DeployedTopologyMetricsCompactResponse)
    else
      getDeployedTopologyMetrics(allCurrentDeployedTopologies, deployedTopology).map(CommandResponse.DeployedTopologyMetricsResponse)
  }
}