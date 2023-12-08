/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.endpoints

import com.mwam.kafkakewl.common.http.EndpointUtils.yamlRequestBody
import com.mwam.kafkakewl.common.http.EndpointUtils
import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.domain.DeploymentsJson.given
import com.mwam.kafkakewl.domain.DeploymentsSchema.given
import sttp.model.StatusCode
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.{oneOfVariant, *}
import sttp.tapir.{Codec, CodecFormat, FieldName, PublicEndpoint, Schema, oneOf}
import zio.*
import zio.json.{JsonDecoder, JsonEncoder}

class DeploymentsEndpoints() extends EndpointUtils {
  private val deploymentsEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = apiEndpoint.in("deployments")
  private val deploymentEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] = apiEndpoint.in("deployment")

  private val queryDeploymentsFailureOutput = oneOf[QueryDeploymentsFailure](
    oneOfVariant(statusCode(StatusCode.NotFound).and(jsonBody[DeploymentsFailure.NotFound])),
    oneOfVariant(statusCode(StatusCode.Unauthorized).and(jsonBody[DeploymentsFailure.Authorization])),
    oneOfVariant(statusCode(StatusCode.RequestTimeout).and(jsonBody[DeploymentsFailure.Timeout]))
  )

  val getDeploymentEndpoint: PublicEndpoint[TopologyId, QueryDeploymentsFailure, TopologyDeployment, Any] = deploymentEndpoint
    .in(path[TopologyId]("topology_id"))
    .get
    .errorOut(queryDeploymentsFailureOutput)
    .out(jsonBody[TopologyDeployment])

  val getDeploymentsEndpoint: PublicEndpoint[TopologyDeploymentQuery, QueryDeploymentsFailure, Seq[TopologyDeployment], Any] = deploymentsEndpoint
    .in(query[Option[String]]("filter"))
    .in(query[Option[Boolean]]("with_topology"))
    .in(query[Option[Int]]("offset"))
    .in(query[Option[Int]]("limit"))
    .mapInTo[TopologyDeploymentQuery]
    .get
    .errorOut(queryDeploymentsFailureOutput)
    .out(jsonBody[Seq[TopologyDeployment]])

  val postDeploymentsEndpoint: PublicEndpoint[Deployments, PostDeploymentsFailure, DeploymentsSuccess, Any] = deploymentsEndpoint
    .in(oneOfBody(jsonBody[Deployments], yamlRequestBody[Deployments]))
    .post
    .errorOut(
      oneOf[PostDeploymentsFailure](
        oneOfVariant(statusCode(StatusCode.Unauthorized).and(jsonBody[DeploymentsFailure.Authorization])),
        oneOfVariant(statusCode(StatusCode.UnprocessableEntity).and(jsonBody[DeploymentsFailure.Validation])),
        oneOfVariant(statusCode(StatusCode.InternalServerError).and(jsonBody[DeploymentsFailure.Deployment])),
        oneOfVariant(statusCode(StatusCode.InternalServerError).and(jsonBody[DeploymentsFailure.Persistence])),
        oneOfVariant(statusCode(StatusCode.RequestTimeout).and(jsonBody[DeploymentsFailure.Timeout]))
      )
    )
    .out(jsonBody[DeploymentsSuccess])
}

object DeploymentsEndpoints {
  val live: ZLayer[Any, Nothing, DeploymentsEndpoints] = ZLayer.succeed(DeploymentsEndpoints())
}
