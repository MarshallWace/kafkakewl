/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import java.time.OffsetDateTime

trait CommandBase {
  val metadata: CommandMetadata
  lazy val mdc: Map[String, String] = Mdc.from(this, metadata)

  def userName: String = metadata.userName
  val dryRun: Boolean = metadata.dryRun
  def timeStampUtc: OffsetDateTime = metadata.timeStampUtc
  def correlationId: String = metadata.correlationId

  /**
    * True if the command can change the state of kafkakewl (it can be true even if dryRun = true)
    */
  def canChangeState: Boolean = false
}

trait CanChangeStateCommand {
  this: CommandBase =>

  override def canChangeState: Boolean = true
}
