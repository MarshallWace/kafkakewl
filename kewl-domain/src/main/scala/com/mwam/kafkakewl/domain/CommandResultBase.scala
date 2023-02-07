/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

trait CommandResultBase {
  val metadata: CommandMetadata
}

object CommandResultBase {
  trait Failed extends CommandResultBase {
    val metadata: CommandMetadata
    val reasons: Seq[CommandError]
  }
}
