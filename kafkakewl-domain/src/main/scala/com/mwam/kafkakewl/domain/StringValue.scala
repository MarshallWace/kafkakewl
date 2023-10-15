/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

/**
 * A universal trait for value classes wrapping strings.
 */
trait StringValue extends Any {
  def value: String
  override def toString: String = value

  def quote: String = s"'$toString'"
}
