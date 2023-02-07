/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import akka.actor.Actor
import com.typesafe.scalalogging.Logger
import com.mwam.kafkakewl.utils._

trait ActorPreRestartLog extends Actor {
  this: MdcUtils =>

  protected val logger: Logger
  val actorMdc: Map[String, Any] = Map.empty

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    ApplicationMetrics.errorCounter.inc()
    withMDC(actorMdc) {
      logger.error(s"preRestart(${reason.toErrorMessage}, $message) starting...")
      super.preRestart(reason, message)
      logger.info(s"preRestart(${reason.toErrorMessage}, $message) finished.")
    }
  }
}
