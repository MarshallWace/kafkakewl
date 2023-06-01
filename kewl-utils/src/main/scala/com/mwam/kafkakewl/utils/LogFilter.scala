/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import ch.qos.logback.classic.turbo.TurboFilter
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.core.spi.FilterReply
import org.slf4j.Marker

class LogFilter extends TurboFilter {
  // some log messages from the spnego library should really be debug-logged only (can't easily change it unless withLog() in akka-http, but then I'd have to restore it inside the spnego stuff...)
  val debugLogRegexes = Seq(
    "^principal .+".r,
    "^keytab .+".r,
    "^debug .+".r,
    "^domain .+".r,
    "^path .+".r,
    "^token validity .+".r,
  )

  def isDebugLog(log: AnyRef): Boolean = {
    log match {
      case stringLog: String if log.ne(null) =>
        debugLogRegexes.exists(_.findFirstMatchIn(stringLog).nonEmpty)
      case _ =>
        false
    }
  }

  override def decide(marker: Marker, logger: Logger, level: Level, format: String, params: Array[AnyRef], t: Throwable): FilterReply = {
    (level, logger.getName) match {
      case (Level.WARN, "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator") if format.contains("Offset commit failed on partition") =>
        // offset commit failed can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.WARN, "org.apache.kafka.clients.admin.AdminClientConfig" | "org.apache.kafka.clients.consumer.ConsumerConfig") if format.contains("supplied but isn't a known config.") =>
        // "The configuration 'sasl.kerberos.kinit.cmd' was supplied but isn't a known config." can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.WARN, "org.apache.kafka.common.security.kerberos.KerberosLogin") if format.contains("TGT renewal thread has been interrupted and will exit") =>
        // TGT renewal can be info
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.ERROR, "SpnegoClient") if format.contains("Failed to get Kerberos ticket end time") =>
        // this kerb4j error can be info, it usually doesn't mean any error
        logger.info(marker, format, params)
        FilterReply.DENY
      case (Level.ERROR, "org.apache.curator.ConnectionState") if format.contains("Authentication failed") =>
        // this curator error can be warning, because we can still talk to ZK un-authenticated
        logger.warn(marker, format, params)
        FilterReply.DENY
      case (Level.INFO, "akka.actor.ActorSystemImpl") if format == "{}" && params.length == 1 && isDebugLog(params(0)) =>
        logger.debug(marker, format, params)
        FilterReply.DENY
      case _ =>
        FilterReply.NEUTRAL
    }
  }
}
