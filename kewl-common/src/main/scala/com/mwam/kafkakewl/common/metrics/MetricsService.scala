/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.metrics

import com.mwam.kafkakewl.domain.ValueOrCommandError
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.metrics.{DeployedTopologyMetrics, DeployedTopologyMetricsCompact}

/**
  * Trait for retrieving metrics from the metrics-service (or other metrics-source)
  */
trait MetricsService {
  def getDeployedTopologiesMetrics(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopologies: Iterable[DeployedTopology]
  ): ValueOrCommandError[Seq[DeployedTopologyMetrics]]

  def getDeployedTopologiesMetricsCompact(
    allCurrentDeployedTopologies: Iterable[DeployedTopology],
    deployedTopologies: Iterable[DeployedTopology]
  ): ValueOrCommandError[Seq[DeployedTopologyMetricsCompact]]
}
