/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.validation.Validation.Result
import com.mwam.kafkakewl.domain.RegexBuilder

trait ValidationUtils {
  implicit class ValidatingStringExtensions(s: String) {
    def validateNonEmpty(name: String): Result =
      if (s.length > 0)
        Validation.Result.success
      else
        Validation.Result.validationError(s"$name cannot be empty.")

    def validateShorterThan(maxLength: Int, name: String): Result =
      if (s.length <= maxLength)
        Validation.Result.success
      else
        Validation.Result.validationError(s"$name '$s' is too long, only $maxLength characters are allowed.")

    def validateRegexWholeString(regex: String, error: String): Result =
      if (RegexBuilder.makeRegexWholeString(regex).r.findFirstIn(s).isDefined)
        Validation.Result.success
      else
        Validation.Result.validationError(s"$error: '$s'.")
  }
}
