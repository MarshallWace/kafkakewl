/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy

import com.mwam.kafkakewl.common.config.loadConfig
import com.mwam.kafkakewl.common.plugins.*
import com.mwam.kafkakewl.deploy.plugins.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*

fun main() {
    val config = loadConfig<Config>(".kafkakewl-deploy-overrides-application.yaml", "/application.yaml")
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

    monitor.subscribe(ApplicationStopped) { application ->
        application.environment.log.info("Ktor server has stopped")
        // TODO can this be done in a better way?
        // TODO this is where we can close AutoCloseable services or do any kind of clean-up tasks
        application.monitor.unsubscribe(ApplicationStopped) {}
    }

    configureRouting()

    configureHealthyHealthCheck("/health/live")
    configureHealthyHealthCheck("/health/ready")
}
