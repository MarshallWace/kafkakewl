/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.endpoints

import com.mwam.kafkakewl.common.http.{EndpointUtils, ErrorResponse}
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.DeploymentsJson.given
import com.mwam.kafkakewl.domain.DeploymentsSchema.given
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*
import sttp.tapir.{EndpointOutput, PublicEndpoint}
import zio.*

class DeploymentsEndpoints() extends EndpointUtils {
  private val deploymentsEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = apiEndpoint.in("deployments")
  private val deploymentEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = apiEndpoint.in("deployment")

  val getDeploymentEndpoint: PublicEndpoint[TopologyId, ErrorResponse, TopologyDeployment, Any] = deploymentEndpoint
    .in(path[TopologyId]("topology_id"))
    .get
    .errorOut(EndpointOutput.derived[ErrorResponse])
    .out(jsonBody[TopologyDeployment])

  val getDeploymentsEndpoint: PublicEndpoint[TopologyDeploymentQuery, Unit, Seq[TopologyDeployment], Any] = deploymentsEndpoint
    .in(query[Option[String]]("filter"))
    .in(query[Option[Boolean]]("with_topology"))
    .in(query[Option[Int]]("offset"))
    .in(query[Option[Int]]("limit"))
    .mapInTo[TopologyDeploymentQuery]
    .get
    .out(jsonBody[Seq[TopologyDeployment]])

  val postDeploymentsEndpoint: PublicEndpoint[Deployments, Unit, DeploymentsResult, Any] = deploymentsEndpoint
    .in(jsonBody[Deployments])
    .post
    .out(jsonBody[DeploymentsResult])
}

object DeploymentsEndpoints {
  val live: ZLayer[Any, Nothing, DeploymentsEndpoints] = ZLayer.succeed(DeploymentsEndpoints())
}
