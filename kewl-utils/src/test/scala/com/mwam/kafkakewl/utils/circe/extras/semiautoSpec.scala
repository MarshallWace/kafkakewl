/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.circe.extras

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}
import org.scalatest.FlatSpec
import shapeless.LabelledGeneric
import com.mwam.kafkakewl.utils.circe.extras.semiauto._

final case class TestCaseClass(topic: String, partitions: Int)

class semiautoSpec extends FlatSpec {
  implicit val customConfig: Configuration = Configuration.default.withDefaults

  "Decoding TestCaseClass with extra fields with the normal Decoder" should "work" in {
    implicit val encoder: Encoder[TestCaseClass] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TestCaseClass] = deriveConfiguredDecoder

    val testCaseClass = TestCaseClass("test-topic1", 100)
    val testCaseClassAsJson = """{"topic":"test-topic1","partitions":100}"""
    assert(testCaseClass.asJson.noSpaces == testCaseClassAsJson)
    assert(testCaseClass == decode[TestCaseClass](testCaseClassAsJson).right.get)

    assert(testCaseClass == decode[TestCaseClass]("""{"topic":"test-topic1","partitions":100,"extra": "hah!"}""").right.get)
    assert(testCaseClass == decode[TestCaseClass]("""{"topic":"test-topic1","partitions":100,"extraObject": {"x": 1}}""").right.get)
  }

  "Decoding TestCaseClass with extra fields with the strict Decoder" should "fail" in {
    implicit val encoder: Encoder[TestCaseClass] = deriveConfiguredEncoder
    implicit val decoder: Decoder[TestCaseClass] = deriveConfiguredDecoder[TestCaseClass].makeStrictFor(LabelledGeneric[TestCaseClass])

    val testCaseClass = TestCaseClass("test-topic1", 100)
    val testCaseClassAsJson = """{"topic":"test-topic1","partitions":100}"""
    assert(testCaseClass.asJson.noSpaces == testCaseClassAsJson)
    assert(testCaseClass == decode[TestCaseClass](testCaseClassAsJson).right.get)

    assert("unexpected json fields of TestCaseClass: 'extra'" == decode[TestCaseClass]("""{"topic":"test-topic1","partitions":100,"extra": "hah!"}""").left.get.getMessage)
    assert("unexpected json fields of TestCaseClass: 'extraObject'" == decode[TestCaseClass]("""{"topic":"test-topic1","partitions":100,"extraObject": {"x": 1}}""").left.get.getMessage)
  }
}
