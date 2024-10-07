/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.plugins

import com.mwam.kafkakewl.domain.Failure
import com.mwam.kafkakewl.metrics.domain.KafkaSingleTopicPartitionInfos
import com.mwam.kafkakewl.metrics.services.KafkaTopicInfoCache
import io.github.oshai.kotlinlogging.withLoggingContext
import io.github.smiley4.ktorswaggerui.SwaggerUI
import io.github.smiley4.ktorswaggerui.dsl.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.webjars.*
import org.koin.ktor.ext.inject
import kotlin.getValue

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
            title = "kafkakewl metrics API"
            version = "latest"
            description = "kafkakewl metrics API"
        }
        server {
            // TODO the service url and description for swagger should come from config
            url = "http://localhost:8080"
            description = "Development Server"
        }
    }
    routing {
        val kafkaTopicInfoCache by inject<KafkaTopicInfoCache>()
        route("/api/v1") {
            get("/topic/{topic}", {
                description = "Returns the specified topic's metadata"
                request {
                    pathParameter<String>("topic") {
                        description = "The topic name"
                    }
                }
                response {
                    HttpStatusCode.OK to {
                        description = "The specified topic's metadata has been returned"
                        body<KafkaSingleTopicPartitionInfos> {
                        }
                    }
                    HttpStatusCode.NotFound to {
                        description = "The specified topic does not exist"
                    }
                }
            }) {
                val topic = call.parameters["topic"]!!
                withLoggingContext("topic" to topic) {
                    val topicPartitionInfos = kafkaTopicInfoCache.getTopicPartitionInfos(topic)
                    if (topicPartitionInfos != null) {
                        call.respond(topicPartitionInfos)
                    } else {
                        call.respond(HttpStatusCode.NotFound, Failure.notFound("topic $topic not found"))
                    }
                }
            }
            get("/topic", {
                description = "Returns all the topic names"
                response {
                    HttpStatusCode.OK to {
                        description = "The topic names have been returned"
                        body<List<String>> {
                        }
                    }
                }
            }) {
                // TODO what should we put in the mdc here?
                withLoggingContext() {
                    val topics = kafkaTopicInfoCache.getTopics()
                    call.respond(topics)
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
