/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import java.time.{Clock, OffsetDateTime}
import java.util.UUID

/**
  * Some metadata for commands containing the user name, timestamp, correlation-id.
  */
final case class CommandMetadata(
  userName: String,
  dryRun: Boolean = false,
  timeStampUtc: OffsetDateTime = OffsetDateTime.now(Clock.systemUTC()),
  correlationId: String = UUID.randomUUID().toString.replace("-", "")
) {
  def withTimeStampUtc(timeStampUtc: OffsetDateTime): CommandMetadata = copy(timeStampUtc = timeStampUtc)
  def withTimeStampUtcNow(): CommandMetadata = withTimeStampUtc(OffsetDateTime.now(Clock.systemUTC()))
  def withNewCorrelationId(): CommandMetadata = copy(correlationId = UUID.randomUUID().toString.replace("-", ""))
}
