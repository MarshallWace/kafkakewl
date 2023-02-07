/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

object RegexBuilder {
  /**
    * Creates a regex string from the input string which matches the whole input.
    *
    * Basically inserts "^" at the beginning if it's not already there and inserts "$" at the end
    * if it's not already there.
    *
    * @param regex the input regex
    * @return the resulting regex that matches whole strings
    */
  def makeRegexWholeString(regex: String): String = {
    val regexPrefix = if (regex.startsWith("^")) "" else "^"
    val regexSuffix = if (regex.endsWith("$")) "" else "$"
    // matching the whole value, making sure the regex is wrapped in ^ and $
    s"$regexPrefix$regex$regexSuffix"
  }
}
