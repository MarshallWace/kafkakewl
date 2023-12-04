/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.logging

import zio.{Config, Runtime, ZLayer}
import zio.logging.slf4j.bridge.Slf4jBridge
import zio.logging.{ConsoleLoggerConfig, FileLoggerConfig, LogFilter, LogFormat}

import java.nio.file.Paths

object Logging {

  private def format = LogFormat.default

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
