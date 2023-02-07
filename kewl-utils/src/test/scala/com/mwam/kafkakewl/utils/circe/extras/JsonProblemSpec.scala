/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.circe.extras

import org.scalatest.FlatSpec

class JsonProblemSpec extends FlatSpec {
  "Decoding Parent with the child having an error" should "fail" in {
    import io.circe.generic.extras.Configuration
    import io.circe.generic.extras.auto._

    implicit val customConfig: Configuration = Configuration.default.withDefaults
    final case class Foo(number: Int = 999)
    println(io.circe.parser.decode[Foo]("""{"number": "not a number"}"""))
  }
}
