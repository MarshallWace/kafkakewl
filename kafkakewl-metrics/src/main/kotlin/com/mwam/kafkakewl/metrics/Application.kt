/*
 * SPDX-FileCopyrightText: 2025 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics

import com.mwam.kafkakewl.common.config.loadConfig
import com.mwam.kafkakewl.common.plugins.*
import com.mwam.kafkakewl.metrics.plugins.*
import com.mwam.kafkakewl.metrics.services.KafkaTopicInfoSource
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import org.koin.ktor.ext.inject
import kotlin.getValue

fun main() {
    val config = loadConfig<Config>(".kafkakewl-metrics-overrides-application.yaml", "/application.yaml")
    initializeLogging(config.kafkaCluster.name)

    embeddedServer(Netty, port = config.http.port, host = config.http.host, module = { module(config) })
        .start(wait = true)
}

fun Application.module(config: Config) {
    configureSecurity()
    configureHTTP()
    configureSerialization()
    configureCallLogging()
    configureFrameworks(config)
    configureMonitoring()
    configureCoreMetrics(config.kafkaCluster.name)

    val kafkaTopicInfoSource by inject<KafkaTopicInfoSource>()
    kafkaTopicInfoSource.startPublishing()

    monitor.subscribe(ApplicationStopped) { application ->
        application.environment.log.info("Ktor server has stopped")
        // TODO can this be done in a better way?
        kafkaTopicInfoSource.close()
        application.monitor.unsubscribe(ApplicationStopped) {}
    }

    configureRouting()

    configureHealthyHealthCheck("/health/live")
    configureHealthyHealthCheck("/health/ready")
}
