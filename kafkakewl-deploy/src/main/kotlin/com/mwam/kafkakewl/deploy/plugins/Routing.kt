/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.plugins

import arrow.core.Either.Left
import arrow.core.Either.Right
import com.mwam.kafkakewl.deploy.services.TopologyDeploymentsService
import com.mwam.kafkakewl.domain.Deployments
import com.mwam.kafkakewl.domain.DeploymentsFailure
import com.mwam.kafkakewl.domain.DeploymentsSuccess
import com.mwam.kafkakewl.domain.Failure
import com.mwam.kafkakewl.domain.TopologyDeployment
import com.mwam.kafkakewl.domain.TopologyDeploymentQuery
import com.mwam.kafkakewl.domain.TopologyId
import io.github.oshai.kotlinlogging.withLoggingContext
import io.github.smiley4.ktorswaggerui.SwaggerUI
import io.github.smiley4.ktorswaggerui.dsl.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.request.receive
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.webjars.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.withContext
import org.koin.ktor.ext.inject

fun Application.configureRouting() {
    install(Webjars) {
        path = "/webjars" //defaults to /webjars
    }
    install(SwaggerUI) {
        swagger {
            swaggerUrl = "swagger-ui"
            forwardRoot = true
        }
        info {
            title = "kafkakewl API"
            version = "latest"
            description = "kafkakewl API"
        }
        server {
            // TODO the service url and description for swagger should come from config
            url = "http://localhost:8080"
            description = "Development Server"
        }
    }
    routing {
        val topologyDeploymentsService by inject<TopologyDeploymentsService>()
        route("/api/v1") {
            get("/deployment/{topologyId}", {
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
                    queryParameter<String?>("filter") {
                        description = "An optional regular expression filter for the topology id"
                    }
                    queryParameter<Boolean?>("with_topology") {
                        description = "Whether the topology should be returned with the deployment or not (defaults to 'true')"
                    }
                    queryParameter<Int?>("offset") {
                        description = "The optional offset (defaults to 0) from which the topology deployments should be returned"
                    }
                    queryParameter<Int?>("limit") {
                        description = "The optional limit (unspecified means no limit) of topology deployments to return"
                    }
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The requested topology deployments have been returned"
                        body<List<TopologyDeployment>> {
                        }
                    }
                }
            }) {
                val filter = call.request.queryParameters["filter"]
                // TODO better parse error handling
                val withTopology = call.request.queryParameters["with_topology"]?.toBoolean()
                val offset = call.request.queryParameters["offset"]?.toInt()
                val limit = call.request.queryParameters["limit"]?.toInt()
                withLoggingContext("filter" to filter, "withTopology" to withTopology?.toString(), "offset" to offset?.toString(), "limit" to limit?.toString()) {
                    val topologyDeploymentFilter = TopologyDeploymentQuery(filter, withTopology, offset, limit)
                    val deployments = topologyDeploymentsService.getTopologyDeployments(topologyDeploymentFilter)
                    call.respond(deployments)
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
                val deployments = call.receive<Deployments>()
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
