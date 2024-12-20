/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.plugins

import com.auth0.jwt.JWT
import com.auth0.jwt.algorithms.Algorithm
import com.mwam.kafkakewl.common.metrics.metricsName
import io.github.smiley4.ktorswaggerui.dsl.*
import io.ktor.http.*
import io.ktor.serialization.JsonConvertException
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.plugins.callloging.processingTimeMillis
import io.ktor.server.plugins.compression.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.cors.routing.*
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.request.receive
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.prometheus.*
import kotlinx.serialization.json.Json
import org.koin.core.module.Module
import org.koin.dsl.module
import org.koin.ktor.ext.inject
import org.slf4j.event.Level

/** initializes the logging, sets some logback variables */
fun initializeLogging(kafkaClusterName: String) {
    System.setProperty("LOGBACK_KAFKA_CLUSTER", kafkaClusterName)
}

/** creates a koin module with the metrics' PrometheusMeterRegistry */
fun koinModuleForMetrics(): Module {
    val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    return module {
        single<MeterRegistry> { meterRegistry }
        single<PrometheusMeterRegistry> { meterRegistry }
    }
}

suspend inline fun <reified T : Any> ApplicationCall.receiveWithDeserializationError(): T =
    try {
        receive<T>()
    } catch (e: BadRequestException) {
        if (e.cause is JsonConvertException) {
            val cause = e.cause!!
            val message = cause.message ?: cause.javaClass.simpleName
            application.log.warn(message)
            respond(HttpStatusCode.BadRequest, message)
            throw BadRequestException(message, cause)
        }
        else {
            throw e
        }
    }

fun Application.configureMonitoring() {
    val meterRegistry by inject<PrometheusMeterRegistry>()

    install(MicrometerMetrics) {
        registry = meterRegistry
        // ...
    }
    routing {
        get("/metrics", {
            hidden = true
        }) {
            call.respond(meterRegistry.scrape())
        }
    }
}

fun Application.configureCoreMetrics(kafkaClusterName: String) {
    val meterRegistry by inject<PrometheusMeterRegistry>()

    // registering the kafka-cluster name as a common metrics tag
    meterRegistry.config().meterFilter(MeterFilter.commonTags(listOf(Tag.of("kafka_cluster", kafkaClusterName))))

    var startTimeMillis = System.currentTimeMillis()

    Gauge.builder(metricsName("uptime"), { System.currentTimeMillis() - startTimeMillis })
        .description("the up-time of the service in milliseconds")
        .register(meterRegistry)
}

fun Application.configureHealthCheck(name: String, isHealth: () -> Boolean) {
    routing {
        get(name, {
            hidden = true
        }) {
            if (isHealth()) {
                call.respond(HttpStatusCode.OK)
            } else {
                call.respond(HttpStatusCode.ServiceUnavailable)
            }
        }
    }
}

fun Application.configureHealthyHealthCheck(name: String) = configureHealthCheck(name) { true }

fun Application.configureHTTP() {
    install(Compression)
    install(CORS) {
        allowMethod(HttpMethod.Options)
        allowMethod(HttpMethod.Put)
        allowMethod(HttpMethod.Delete)
        allowMethod(HttpMethod.Patch)
        allowHeader(HttpHeaders.Authorization)
        allowHeader("MyCustomHeader")
        anyHost() // @TODO: Don't do this in production if possible. Try to limit it.
    }
}

fun Application.configureSecurity() {
    // Please read the jwt property from the config file if you are using EngineMain
    val jwtAudience = "jwt-audience"
    val jwtDomain = "https://jwt-provider-domain/"
    val jwtRealm = "ktor sample app"
    val jwtSecret = "secret"
    authentication {
        jwt {
            realm = jwtRealm
            verifier(
                JWT
                    .require(Algorithm.HMAC256(jwtSecret))
                    .withAudience(jwtAudience)
                    .withIssuer(jwtDomain)
                    .build()
            )
            validate { credential ->
                if (credential.payload.audience.contains(jwtAudience)) JWTPrincipal(credential.payload) else null
            }
        }
    }
}

fun Application.configureSerialization() {
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
            isLenient = true
        })
// TODO jackson or kotlinx.serialization.json?
//        jackson {
//            enable(SerializationFeature.INDENT_OUTPUT)
//        }
    }
}

fun Application.configureCallLogging() {
    install(CallLogging) {
        level = Level.INFO
        filter { call ->
            call.request.path().startsWith("/api/v1")
        }
        format { call ->
            val httpMethod = call.request.httpMethod.value
            val path = call.request.path()
            val status = call.response.status()
            val processingTimeMillis = call.processingTimeMillis()
            // TODO log more fields, e.g. some headers, username, etc...?
            "$httpMethod $path -> $status ($processingTimeMillis ms)"
        }
        mdc("httpMethod") { call -> call.request.httpMethod.value }
        mdc("httpPath") { call -> call.request.path() }
    }
}
