/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import java.time.{Clock, OffsetDateTime}

final case class WithUtcTimestamp[T](timestampUtc: OffsetDateTime, value: T)

object WithUtcTimestamp {
  def apply[T](value: T): WithUtcTimestamp[T] = new WithUtcTimestamp(OffsetDateTime.now(Clock.systemUTC()), value)
}
