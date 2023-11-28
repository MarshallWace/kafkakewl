/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.endpoints

import com.mwam.kafkakewl.common.telemetry.zServerLogicWithTracing
import com.mwam.kafkakewl.deploy.services.TopologyDeploymentsService
import com.mwam.kafkakewl.domain.*
import sttp.tapir.ztapir.*
import zio.*
import zio.telemetry.opentelemetry.tracing.Tracing

class DeploymentsServerEndpoints(
    deploymentsEndpoints: DeploymentsEndpoints,
    topologyDeploymentsService: TopologyDeploymentsService,
    tracing: Tracing
) {
  given Tracing = tracing

  val endpoints: List[ZServerEndpoint[Any, Any]] = List(
    deploymentsEndpoints.getDeploymentEndpoint.zServerLogicWithTracing(getDeployment),
    deploymentsEndpoints.getDeploymentsEndpoint.zServerLogicWithTracing(getDeployments),
    deploymentsEndpoints.postDeploymentsEndpoint.zServerLogicWithTracing(postDeployments)
  )

  private def getDeployment(topologyId: TopologyId): ZIO[Any, QueryDeploymentsFailure, TopologyDeployment] = for {
    _ <- tracing.addEvent("reading topology from cache")
    _ <- tracing.setAttribute("topology_id", topologyId.value)
    topologyDeployment <- topologyDeploymentsService.getTopologyDeployment(topologyId)
    _ <- tracing.addEvent(topologyDeployment match
      case Some(_) => "read topology from cache"
      case None    => "topology not found in cache"
    )
    topologyDeployment <- ZIO.getOrFailWith(DeploymentsFailure.notFound(s"topology $topologyId not found"))(topologyDeployment)
  } yield topologyDeployment

  private def getDeployments(deploymentQuery: TopologyDeploymentQuery): ZIO[Any, QueryDeploymentsFailure, Seq[TopologyDeployment]] =
    topologyDeploymentsService.getTopologyDeployments(deploymentQuery)

  private def postDeployments(deployments: Deployments): ZIO[Any, PostDeploymentsFailure, DeploymentsSuccess] =
    topologyDeploymentsService.deploy(deployments)
}

object DeploymentsServerEndpoints {
  val live: ZLayer[DeploymentsEndpoints & TopologyDeploymentsService & Tracing, Nothing, DeploymentsServerEndpoints] =
    ZLayer.fromFunction(DeploymentsServerEndpoints(_, _, _))
}
