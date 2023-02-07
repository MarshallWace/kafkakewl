/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.JsonEncodeDecodeBase._
import io.circe.parser._
import io.circe.syntax._
import org.scalatest.FlatSpec

class FlexibleNameEncoderDecodersSpec extends FlatSpec {
  "FlexibleName.Exact" should "be encoded as a string" in {
    val flexibleName: FlexibleName = FlexibleName.Exact("something")
    val flexibleNameAsJson = """"something""""
    assert(flexibleName.asJson.noSpaces == flexibleNameAsJson)
    assert(flexibleName == decode[FlexibleName](flexibleNameAsJson).right.get)

    // other ways to decode into the same flexible-name
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something"}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something","any":false}""").right.get)
    // even in these cases it's exact
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something","regex":"\\d+"}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something","prefix":"xyz."}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something","namespace":"xyz"}""").right.get)
  }

  "FlexibleName.Any" should "be encoded as a any: true" in {
    val flexibleName: FlexibleName = FlexibleName.Any()
    val flexibleNameAsJson = """{"any":true}"""
    assert(flexibleName.asJson.noSpaces == flexibleNameAsJson)
    assert(flexibleName == decode[FlexibleName](flexibleNameAsJson).right.get)

    // other ways to decode into the same flexible-name
    assert(flexibleName == decode[FlexibleName]("""{"exact":"something","any":true}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"regex":"\\d+","any":true}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"prefix":"xyz.","any":true}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"namespace":"xyz","any":true}""").right.get)
  }

  "FlexibleName.Regex" should "be encoded as a regex: value" in {
    val flexibleName: FlexibleName = FlexibleName.Regex(".*")
    val flexibleNameAsJson = """{"regex":".*"}"""
    assert(flexibleName.asJson.noSpaces == flexibleNameAsJson)
    assert(flexibleName == decode[FlexibleName](flexibleNameAsJson).right.get)

    // other ways to decode into the same flexible-name
    assert(flexibleName == decode[FlexibleName]("""{"regex":".*","any":false}""").right.get)
    // even in these cases it's regex
    assert(flexibleName == decode[FlexibleName]("""{"regex":".*","prefix":"xyz."}""").right.get)
    assert(flexibleName == decode[FlexibleName]("""{"regex":".*","namespace":"xyz"}""").right.get)
  }

  "FlexibleName.Prefix" should "be encoded as a prefix: value" in {
    val flexibleName: FlexibleName = FlexibleName.Prefix("xyz.")
    val flexibleNameAsJson = """{"prefix":"xyz."}"""
    assert(flexibleName.asJson.noSpaces == flexibleNameAsJson)
    assert(flexibleName == decode[FlexibleName](flexibleNameAsJson).right.get)

    // other ways to decode into the same flexible-name
    assert(flexibleName == decode[FlexibleName]("""{"prefix":"xyz.","any":false}""").right.get)
    // even in these cases it's regex
    assert(flexibleName == decode[FlexibleName]("""{"prefix":"xyz.","namespace":"xyz"}""").right.get)
  }

  "FlexibleName.Namespace" should "be encoded as a project: value" in {
    val flexibleName: FlexibleName = FlexibleName.Namespace("xyz")
    val flexibleNameAsJson = """{"namespace":"xyz"}"""
    assert(flexibleName.asJson.noSpaces == flexibleNameAsJson)
    assert(flexibleName == decode[FlexibleName](flexibleNameAsJson).right.get)

    // other ways to decode into the same flexible-name
    assert(flexibleName == decode[FlexibleName]("""{"namespace":"xyz","any":false}""").right.get)
  }
}
