/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.metrics.endpoints

import com.mwam.kafkakewl.metrics.domain.{Failures, QueryFailure}
import com.mwam.kafkakewl.metrics.domain.FailuresJson.given
import sttp.model.StatusCode
import sttp.tapir.EndpointOutput
import sttp.tapir.generic.auto.*
import sttp.tapir.json.zio.*
import sttp.tapir.ztapir.*

trait EndpointOutputs {
  val queryFailureOutput: EndpointOutput.OneOf[QueryFailure, QueryFailure] =
    oneOf[QueryFailure](
      oneOfVariant(
        statusCode(StatusCode.NotFound).and(jsonBody[Failures.NotFound])
      ),
      oneOfVariant(
        statusCode(StatusCode.Unauthorized).and(
          jsonBody[Failures.Authorization]
        )
      )
    )
}
