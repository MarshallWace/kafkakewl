/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.config

import com.typesafe.config.{Config, ConfigFactory}
import zio.*

import java.io.File

object TypesafeConfigExtensions {
  def loadWithOverride(overrideFilePath: String): Task[Config] =
    for {
      file <- ZIO.attempt(new File(overrideFilePath))
      defaultConfig <- ZIO.attempt(ConfigFactory.load())
      overrideConfig <- if (file.exists()) ZIO.attempt(ConfigFactory.parseFile(file)) else ZIO.succeed(ConfigFactory.empty)
      mergedConfig <- ZIO.attempt(overrideConfig.withFallback(defaultConfig).resolve)
    } yield mergedConfig
}
