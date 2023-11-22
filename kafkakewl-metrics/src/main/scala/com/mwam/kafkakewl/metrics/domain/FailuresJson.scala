/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.domain

import zio.json.{DeriveJsonDecoder, DeriveJsonEncoder, JsonDecoder, JsonEncoder}

object FailuresJson {
  given JsonEncoder[Failures.NotFound] =
    DeriveJsonEncoder.gen[Failures.NotFound]
  given JsonDecoder[Failures.NotFound] =
    DeriveJsonDecoder.gen[Failures.NotFound]
  given JsonEncoder[Failures.Authorization] =
    DeriveJsonEncoder.gen[Failures.Authorization]
  given JsonDecoder[Failures.Authorization] =
    DeriveJsonDecoder.gen[Failures.Authorization]
}
