/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.utils._

/**
  * Base trait for all id types that are not entity ids.
  */
trait IdLike extends Any {
  def id: String
  override def toString: String = id
  def isEmpty: Boolean = id.trim.isEmpty
  def quote: String = id.quote

  def asIdLike: IdLike = this
}
