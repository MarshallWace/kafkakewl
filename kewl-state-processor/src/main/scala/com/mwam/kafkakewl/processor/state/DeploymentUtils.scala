/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import com.mwam.kafkakewl.common.ReadableVersionedStateStore
import com.mwam.kafkakewl.domain.deploy.{Deployment, DeploymentChange, DeploymentChangeTopologyVersion, DeploymentTopologyVersion}
import com.mwam.kafkakewl.domain.topology.Topology

object DeploymentUtils {
  /**
    * If the deployment change's topologyVersion is DeploymentTopologyVersion.LatestOnce, it returns one with the exact current latest version.
    *
    * This is needed so that the deployments we store always contain either the exact version or latest-tracking.
    *
    * @param topologyStateStore the topology state store
    * @param deploymentChange the deployment change
    * @return the new deployment with DeploymentTopologyVersion.LatestOnce resolved to an exact version.
    */
  def resolveDeploymentLatestOnce(
    topologyStateStore: ReadableVersionedStateStore[Topology],
    deploymentChange: DeploymentChange): Deployment = {

    val deploymentTopologyVersion = deploymentChange.topologyVersion match {
      case _: DeploymentChangeTopologyVersion.Remove => DeploymentTopologyVersion.Remove()
      case v: DeploymentChangeTopologyVersion.Exact => DeploymentTopologyVersion.Exact(v.exact)
      case _: DeploymentChangeTopologyVersion.LatestOnce =>
        val topology = topologyStateStore.getLatestLiveState(deploymentChange.topologyId)
          .getOrElse { throw new RuntimeException(s"No live topology with id=${deploymentChange.topologyId}. The validation code should have spotted it.") }
        DeploymentTopologyVersion.Exact(topology.version)
      case _: DeploymentChangeTopologyVersion.LatestTracking => DeploymentTopologyVersion.LatestTracking()
    }

    Deployment(
      deploymentChange.kafkaClusterId,
      deploymentChange.topologyId,
      deploymentTopologyVersion,
      deploymentChange.tags,
      deploymentChange.labels
    )
  }
}
