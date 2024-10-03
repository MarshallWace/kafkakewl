/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.logging

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.core.spi.FilterReply
import org.slf4j.Marker

class LogFilter : TurboFilter() {
    override fun decide(
        marker: Marker?,
        logger: Logger,
        level: Level,
        format: String?,
        params: Array<Any>?,
        t: Throwable?
    ): FilterReply {
        fun logInfo() {
            if (params == null) {
                logger.info(marker, format)
            } else {
                logger.info(marker, format, *params)
            }
        }

        fun logWarn() {
            if (params == null) {
                logger.warn(marker, format)
            } else {
                logger.warn(marker, format, *params)
            }
        }

        return when {
            level == Level.WARN && logger.name == "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator" && format?.contains("Offset commit failed on partition") == true -> {
                // offset commit failed can be info
                logInfo()
                FilterReply.DENY
            }
            level == Level.WARN && (logger.name == "org.apache.kafka.clients.admin.AdminClientConfig" || logger.name == "org.apache.kafka.clients.consumer.ConsumerConfig") && format?.contains("supplied but isn't a known config.") == true -> {
                // "The configuration 'sasl.kerberos.kinit.cmd' was supplied but isn't a known config." can be info
                logInfo()
                FilterReply.DENY
            }
            level == Level.WARN && logger.name == "org.apache.kafka.common.security.kerberos.KerberosLogin" && format?.contains("TGT renewal thread has been interrupted and will exit") == true -> {
                // TGT renewal can be info
                logInfo()
                FilterReply.DENY
            }
            level == Level.ERROR && logger.name == "org.apache.curator.ConnectionState" && format?.contains("Authentication failed") == true -> {
                // this curator error can be warning, because we can still talk to ZK un-authenticated
                logWarn()
                FilterReply.DENY
            }
            else -> FilterReply.NEUTRAL
        }
    }
}
