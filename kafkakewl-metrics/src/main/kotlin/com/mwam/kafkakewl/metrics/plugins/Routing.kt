/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.plugins

import com.mwam.kafkakewl.domain.*
import com.mwam.kafkakewl.metrics.domain.*
import com.mwam.kafkakewl.metrics.services.*
import io.github.oshai.kotlinlogging.withLoggingContext
import io.github.smiley4.ktoropenapi.*
import io.github.smiley4.ktorswaggerui.swaggerUI
import io.ktor.http.*
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
    install(OpenApi)

    routing {
        route("api.json") {
            openApi()
        }
        route("swagger") {
            swaggerUI("/api.json") {
            }
        }

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
