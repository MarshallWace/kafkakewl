/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics

import com.mwam.kafkakewl.common.plugins.*
import com.mwam.kafkakewl.metrics.plugins.*
import com.mwam.kafkakewl.metrics.services.KafkaTopicInfoSource
import com.sksamuel.hoplite.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.koin.ktor.ext.inject
import kotlin.getValue

fun main() {
    val config = ConfigLoaderBuilder.default()
        // TODO this fails currently because there are weird env vars on my linux box
        //.addEnvironmentSource(useUnderscoresAsSeparator = true)
        .addFileSource(".kafkakewl-metrics-overrides-application.yaml")
        .addResourceSource("/application.yaml")
        .build()
        .loadConfigOrThrow<Config>()

    embeddedServer(Netty, port = config.http.port, host = config.http.host, module = { module(config) })
        .start(wait = true)
}

fun Application.module(config: Config) {
    configureSecurity()
    configureHTTP()
    configureMonitoring()
    configureSerialization()
    configureCallLogging()
    configureFrameworks(config)

    val kafkaTopicInfoSource by inject<KafkaTopicInfoSource>()
    kafkaTopicInfoSource.startPublishing()

    environment.monitor.subscribe(ApplicationStopped) { application ->
        application.environment.log.info("Ktor server has stopped")
        // TODO can this be done in a better way?
        kafkaTopicInfoSource.close()
        application.environment.monitor.unsubscribe(ApplicationStopped) {}
    }

    configureRouting()

    configureHealthyHealthCheck("live")
    configureHealthyHealthCheck("ready")
}
