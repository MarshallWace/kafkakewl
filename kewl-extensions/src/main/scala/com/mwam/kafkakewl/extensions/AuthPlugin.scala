/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.extensions

import akka.http.scaladsl.server.Directive1
import com.typesafe.config.Config

/**
  * Extension interface to create an akka-http directive to authenticate.
  */
trait AuthPlugin extends Plugin {
  /**
    * Creates an akka-http directive with a single string output (the username) that authenticates.
    *
    * @param config the type-safe config to use to load any configuration for the plugin
    */
  def createAuthenticationDirective(config: Config): Directive1[String]
}
