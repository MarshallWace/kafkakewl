/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.logging

import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.spi.FilterReply
import org.slf4j.Marker

class LogFilter extends TurboFilter {
  override def decide(marker: Marker, logger: Logger, level: Level, format: String, params: Array[AnyRef], t: Throwable): FilterReply = {
    (level, logger.getName) match {
      case (Level.WARN, "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator") if format.contains("Offset commit failed on partition") =>
        // offset commit failed can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.WARN, "org.apache.kafka.clients.admin.AdminClientConfig" | "org.apache.kafka.clients.consumer.ConsumerConfig")
          if format.contains("supplied but isn't a known config.") =>
        // "The configuration 'sasl.kerberos.kinit.cmd' was supplied but isn't a known config." can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.WARN, "org.apache.kafka.common.security.kerberos.KerberosLogin")
          if format.contains("TGT renewal thread has been interrupted and will exit") =>
        // TGT renewal can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.ERROR, "org.apache.curator.ConnectionState") if format.contains("Authentication failed") =>
        // this curator error can be warning, because we can still talk to ZK un-authenticated
        logger.warn(marker, format, params)
        FilterReply.DENY
      case _ =>
        FilterReply.NEUTRAL
    }
  }
}
