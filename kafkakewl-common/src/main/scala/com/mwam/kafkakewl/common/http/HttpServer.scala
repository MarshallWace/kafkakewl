/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import com.mwam.kafkakewl.domain.config.HttpConfig
import zio.*
import zio.http.Server

object HttpServer {
  val live: ZLayer[HttpConfig, Throwable, Server] = for {
    httpConfigEnv <- ZLayer.service[HttpConfig]
    server <- Server.defaultWithPort(httpConfigEnv.get.port)
  } yield server
}
