/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config

trait HttpConfigProvider {
  val configForHttp: Config

  lazy val httpPort: Int = configForHttp.getInt(s"http-port")
  lazy val httpAllowedOrigins: Seq[String] = configForHttp.getStringOrNoneIfEmpty(s"http-allowed-origins").getOrElse("").split(",").map(_.trim).filter(_.nonEmpty)
  lazy val authPluginName: String = configForHttp.getStringOrNoneIfEmpty(s"auth-plugin-name").getOrElse(sys.error(s"auth-plugin-name config value must be set"))
}
