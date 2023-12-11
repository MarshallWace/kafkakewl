/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.logging

import zio.*
import zio.logging.LogFormat.{quoted, space, *}
import zio.{Config, Runtime, ZLayer}
import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.{ConsoleLoggerConfig, FileLoggerConfig, LogColor, LogFilter, LogFormat, LoggerNameExtractor}

import java.nio.file.Paths

object Logging {

  private def levelMapper: LogFormat =
    LogFormat.make { (builder, trace, _, logLevel, message, _, fibreRefs, _, annotations) =>
      {
        val loggerName = LoggerNameExtractor.loggerNameAnnotationOrTrace(trace, fibreRefs, annotations).getOrElse("zio-logger")

        val newLogLevel = (logLevel, loggerName) match {
          case (LogLevel.Warning, "org.apache.kafka.clients.consumer.internals.ConsumerCoordinator")
              if message().contains("Offset commit failed on partition") =>
            LogLevel.Info
          case (LogLevel.Warning, "org.apache.kafka.clients.admin.AdminClientConfig" | "org.apache.kafka.clients.consumer.ConsumerConfig")
              if message().contains("supplied but isn't a known config.") =>
            LogLevel.Info
          case (LogLevel.Warning, "org.apache.kafka.common.security.kerberos.KerberosLogin")
              if message().contains("TGT renewal thread has been interrupted and will exit") =>
            LogLevel.Info
          case (LogLevel.Error, "org.apache.curator.ConnectionState") if message().contains("Authentication failed") =>
            LogLevel.Warning
          case _ => logLevel
        }

        builder.appendText(newLogLevel.label)

      }
    }

  private def format = label("timestamp", timestamp.fixed(32)).color(LogColor.BLUE) |-|
    label("level", levelMapper).highlight |-|
    label("thread", fiberId).color(LogColor.WHITE) |-|
    label("message", quoted(line)).highlight +
    (space + label("cause", cause).highlight).filter(LogFilter.causeNonEmpty) |-| allAnnotations

  private def filter =
    LogFilter.acceptAll

  private def fileLogger =
    zio.logging.fileJsonLogger(
      new FileLoggerConfig(
        Paths.get("logs/kafkakewl-deploy.log"),
        format,
        filter,
        rollingPolicy = Some(FileLoggerConfig.FileRollingPolicy.TimeBasedRollingPolicy)
      )
    )

  private def consoleLogger =
    zio.logging.consoleLogger(ConsoleLoggerConfig(format, filter))

  private def consoleJsonLogger = zio.logging.consoleJsonLogger()

  def localLogger: ZLayer[Any, Config.Error, Unit] = Runtime.removeDefaultLoggers >>> (fileLogger ++ consoleLogger) >+> Slf4jBridge.initialize

  def deployLogger: ZLayer[Any, Config.Error, Unit] = Runtime.removeDefaultLoggers >>> consoleJsonLogger >+> Slf4jBridge.initialize

}
