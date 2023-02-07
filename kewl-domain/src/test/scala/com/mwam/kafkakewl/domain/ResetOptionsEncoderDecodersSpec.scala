/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.deploy.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.{ResetApplicationOptions, ResetOptions}
import com.mwam.kafkakewl.domain.topology.LocalApplicationId
import io.circe.syntax._
import io.circe.parser._
import org.scalatest.FlatSpec

class ResetOptionsEncoderDecodersSpec extends FlatSpec with TestTopologiesCommon with TestTopologiesToDeployCommon {
  "ResetApplicationOptions.ApplicationTopics" should "be encoded/decoded correctly" in {
    val resetOptions = ResetOptions(LocalApplicationId("kewltest.sink"),ResetApplicationOptions.ApplicationTopics())
    val resetOptionsAsJson = """{"application":"kewltest.sink","options":{"ApplicationTopics":{"topics":[{"any":true}],"position":{"default":true},"deleteKafkaStreamsInternalTopics":[{"any":true}]}},"authorizationCode":null}"""
    assert(resetOptions.asJson.noSpaces == resetOptionsAsJson)
    assert(resetOptions == decode[ResetOptions](resetOptionsAsJson).right.get)

    // other ways to decode into the same
    assert(resetOptions == decode[ResetOptions]("""{"application":"kewltest.sink","options":{"ApplicationTopics":{}}}""").right.get)
  }

  "ResetApplicationOptions.Connector" should "be encoded/decoded correctly" in {
    val resetOptions = ResetOptions(LocalApplicationId("kewltest.sink"), ResetApplicationOptions.Connector())
    val resetOptionsAsJson = """{"application":"kewltest.sink","options":{"Connector":{"keyRegex":null}},"authorizationCode":null}"""
    assert(resetOptions.asJson.noSpaces == resetOptionsAsJson)
    assert(resetOptions == decode[ResetOptions](resetOptionsAsJson).right.get)
  }

  "ResetApplicationOptions.ConnectReplicator" should "be encoded/decoded correctly" in {
    val resetOptions = ResetOptions(LocalApplicationId("kewltest.sink"), ResetApplicationOptions.ConnectReplicator())
    val resetOptionsAsJson = """{"application":"kewltest.sink","options":{"ConnectReplicator":{"topic":null,"partition":null}},"authorizationCode":null}"""
    assert(resetOptions.asJson.noSpaces == resetOptionsAsJson)
    assert(resetOptions == decode[ResetOptions](resetOptionsAsJson).right.get)

    // other ways to decode into the same
    assert(resetOptions == decode[ResetOptions]("""{"application":"kewltest.sink","options":{"ConnectReplicator":{}}}""").right.get)
  }

  "ResetApplicationOptions.ConnectReplicator with a topic" should "be encoded/decoded correctly" in {
    val resetOptions = ResetOptions(LocalApplicationId("kewltest.sink"), ResetApplicationOptions.ConnectReplicator(topic = Some("topic-name")))
    val resetOptionsAsJson = """{"application":"kewltest.sink","options":{"ConnectReplicator":{"topic":"topic-name","partition":null}},"authorizationCode":null}"""
    assert(resetOptions.asJson.noSpaces == resetOptionsAsJson)
    assert(resetOptions == decode[ResetOptions](resetOptionsAsJson).right.get)

    // other ways to decode into the same
    assert(resetOptions == decode[ResetOptions]("""{"application":"kewltest.sink","options":{"ConnectReplicator":{"topic":"topic-name"}}}""").right.get)
  }
}
