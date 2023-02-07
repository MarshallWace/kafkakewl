/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.typesafe.config.{Config, ConfigFactory}

trait ConfigProvider {
  private def parseFileFromEnvVar(env: String, defaultFileName: String): Config = {
    val fileName = Option(System.getenv(env)).getOrElse(defaultFileName)
    val file = new java.io.File(fileName)
    val fileExists = file.exists()
    ConfigFactory.parseFile(file)
  }


  def loadConfigWithDefaultsAndOverrides(prefix: String): Config = {
    val upperCasePrefix = prefix.toUpperCase.replace("-", "_")
    parseFileFromEnvVar(env = s"${upperCasePrefix}_CONF_OVERRIDES", defaultFileName = s"$prefix-overrides.conf")
      .withFallback(ConfigFactory.load())
      .withFallback(parseFileFromEnvVar(env = s"${upperCasePrefix}_CONF_DEFAULTS", defaultFileName = s"$prefix-defaults.conf"))
      .resolve()
  }
}
