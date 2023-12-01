/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.services

import com.mwam.kafkakewl.domain.{Deployments, DeploymentsSuccess, TopologyDeploymentStatus}
import zio.*

class TopologyDeploymentsToKafkaService {
  def deploy(deployments: Deployments): Task[DeploymentsSuccess] = {
    ZIO.succeed {
      // For now we just return empty statuses for all deploy/delete topologies
      DeploymentsSuccess(
        deployments.deploy.map(topology => (topology.id, TopologyDeploymentStatus())).toMap ++
          deployments.delete.map(tid => (tid, TopologyDeploymentStatus())).toMap
      )
    }
  }
}

object TopologyDeploymentsToKafkaService {
  def live: ZLayer[Any, Nothing, TopologyDeploymentsToKafkaService] =
    // TODO proper dependencies, etc...
    ZLayer.succeed {
      TopologyDeploymentsToKafkaService()
    }
}
