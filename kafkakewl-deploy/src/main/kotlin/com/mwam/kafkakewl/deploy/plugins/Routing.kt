/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.plugins

import arrow.core.Either.*
import com.mwam.kafkakewl.common.plugins.receiveWithDeserializationError
import com.mwam.kafkakewl.deploy.services.*
import com.mwam.kafkakewl.domain.*
import io.github.oshai.kotlinlogging.withLoggingContext
import io.github.smiley4.ktoropenapi.*
import io.github.smiley4.ktoropenapi.config.RequestConfig
import io.github.smiley4.ktorswaggerui.swaggerUI
import io.ktor.http.*
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.webjars.*
import org.koin.ktor.ext.inject

/** extracts the filter/offset/limit query parameters from the request */
fun ApplicationRequest.extractTopologyDeploymentQuery(): TopologyDeploymentQuery {
    val filter = call.request.queryParameters["filter"]
    // TODO better parse error handling
    val offset = call.request.queryParameters["offset"]?.toInt()
    val limit = call.request.queryParameters["limit"]?.toInt()
    return TopologyDeploymentQuery(filter, offset, limit)
}

/** describes the filter/offset/limit query parameters for an OpenApi request */
fun RequestConfig.queryParametersForTopologyDeploymentQuery() {
    queryParameter<String?>("filter") {
        description = "An optional regular expression filter for the topology id"
    }
    queryParameter<Int?>("offset") {
        description = "The optional offset (defaults to 0) from which the topology deployments should be returned"
    }
    queryParameter<Int?>("limit") {
        description = "The optional limit (unspecified means no limit) of topology deployments to return"
    }
}

fun Application.configureRouting() {
    install(Webjars) {
        path = "/webjars" //defaults to /webjars
    }
    install(OpenApi)

    routing {
        route("api.json") {
            openApi()
        }
        route("swagger") {
            swaggerUI("/api.json") {
            }
        }

        val topologyDeploymentsService by inject<TopologyDeploymentsService>()

        route("/api/v1") {
            get("/deployments/{topologyId}", {
                description = "Returns the specified topology"
                request {
                    pathParameter<String>("topologyId") {
                        description = "The topology id"
                    }
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The specified topology's latest deployment has been returned"
                        body<TopologyDeployment> {
                        }
                    }
                    HttpStatusCode.NotFound to {
                        description = "The specified topology hasn't been deployed"
                    }
                }
            }) {
                val topologyId = TopologyId(call.parameters["topologyId"]!!)
                withLoggingContext("topologyId" to topologyId.value) {
                    val topologyDeployment = topologyDeploymentsService.getTopologyDeployment(topologyId)
                    if (topologyDeployment != null) {
                        call.respond(topologyDeployment)
                    } else {
                        call.respond(HttpStatusCode.NotFound, Failure.notFound("topology $topologyId not found"))
                    }
                }
            }
            get("/deployments", {
                description = "Returns the deployments for the optional filter parameters"
                request {
                    queryParametersForTopologyDeploymentQuery()
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The requested topology deployments have been returned"
                        body<List<TopologyDeployment>> {
                        }
                    }
                }
            }) {
                val query = call.request.extractTopologyDeploymentQuery()
                withLoggingContext("filter" to query.topologyIdFilterRegex, "offset" to query.offset?.toString(), "limit" to query.limit?.toString()) {
                    val deployments = topologyDeploymentsService.getTopologyDeployments(query)
                    call.respond(deployments)
                }
            }
            get("/deployments-compact", {
                description = "Returns the compact deployments (without the actual topology) for the optional filter parameters"
                request {
                    queryParametersForTopologyDeploymentQuery()
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The requested topology deployments have been returned"
                        body<List<TopologyDeploymentCompact>> {
                        }
                    }
                }
            }) {
                val query = call.request.extractTopologyDeploymentQuery()
                withLoggingContext("filter" to query.topologyIdFilterRegex, "offset" to query.offset?.toString(), "limit" to query.limit?.toString()) {
                    val deployments = topologyDeploymentsService.getTopologyDeployments(query)
                    val deploymentsCompact = deployments.map { it.toCompact() }
                    call.respond(deploymentsCompact)
                }
            }
            post("/deployments", {
                description = "Deploys the specified topologies"
                request {
                    body<Deployments>() {
                        description = "The deployment with the topologies to deploy and remove"
                    }
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The deployment was successful"
                        body<DeploymentsSuccess> {
                        }
                    }
                    HttpStatusCode.Unauthorized to {
                        description = "The deployment has not been authorized for the user"
                        body<Failure.Authorization> {
                        }
                    }
                    HttpStatusCode.BadRequest to {
                        description = "Invalid deployment"
                        body<DeploymentsFailure.Validation> {
                        }
                    }
                    HttpStatusCode.InternalServerError to {
                        description = "The deployment has failed"
                        body<DeploymentsFailure.Deployment> {
                        }
                    }
                }
            }) {
                val deployments = call.receiveWithDeserializationError<Deployments>()
                // TODO add other fields into the MDC
                withLoggingContext("dryRun" to deployments.options.dryRun.toString(), "allowUnsafe" to deployments.options.allowUnsafe.toString()) {
                    val deploymentResult = topologyDeploymentsService.deploy(deployments)
                    when (deploymentResult) {
                        is Left -> when (deploymentResult.value) {
                            is Failure.Authorization -> call.respond(HttpStatusCode.Unauthorized, deploymentResult.value)
                            is DeploymentsFailure.Validation -> call.respond(HttpStatusCode.BadRequest, deploymentResult.value)
                            else -> call.respond(HttpStatusCode.InternalServerError, deploymentResult.value)
                        }
                        is Right -> call.respond(deploymentResult.value)
                    }
                }
            }
        }
        get("/webjars", {
            hidden = true
        }) {
            call.respondText("<script src='/webjars/jquery/jquery.js'></script>", ContentType.Text.Html)
        }
    }
}
