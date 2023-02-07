/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import java.time.LocalDateTime

import cats.data.NonEmptyList
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import org.scalatest.FlatSpec

class AuthorizationCodeSpec extends FlatSpec {
  import AuthorizationCode._

  val prodCluster = KafkaClusterEntityId("prod-cluster")
  val stagingCluster = KafkaClusterEntityId("staging-cluster")

  val userX = "userX"
  val userY = "userY"

  val stateA = "a"
  val stateB = "b"

  val time = LocalDateTime.parse("2019-01-11T15:37:21.123456789")

  val input = Generator.inputFor _

  it should "correctly generate the current and the last 5 codes without hashing" in {
    val actualCodes = Generator.generate(input(prodCluster, userX, stateA), time, numberOfPreviousResults = 5, hashFunc = identity)
    val expectedCodes = NonEmptyList.of(
      "prod-cluster/userX[a]: 2019-01-11T15:37",
      "prod-cluster/userX[a]: 2019-01-11T15:36",
      "prod-cluster/userX[a]: 2019-01-11T15:35",
      "prod-cluster/userX[a]: 2019-01-11T15:34",
      "prod-cluster/userX[a]: 2019-01-11T15:33",
      "prod-cluster/userX[a]: 2019-01-11T15:32"
    )
    assert(actualCodes == expectedCodes)
  }

  it should "correctly generate the current and the last 5 codes with hashing" in {
    val actualCodes = Generator.generateByMinutesSha256Truncated10(input(prodCluster, userX, stateA), time, 5)
    val expectedCodes = NonEmptyList.of(
      "ac7da16469fe93ee",
      "c40a9ebf1d1a242e",
      "432277de233d5793",
      "2c084d5cae6f845b",
      "abcf5bc1357d2139",
      "a67cb606af2f6a83"
    )
    assert(actualCodes == expectedCodes)
  }

  it should "correctly detect that an old code is no longer valid" in {
    // 30 seconds later
    val timePlus30Seconds = time.plusSeconds(30)

    // the current one is still, no matter how many minutes we look back
    for (minutes <- 0 to 60) assert(isValidInLastMinutes("ac7da16469fe93ee", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))

    // the t - 1 is only valid if we go back more than 0 minute
    for (minutes <- 0 to 0) assert(!isValidInLastMinutes("c40a9ebf1d1a242e", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
    for (minutes <- 1 to 60) assert(isValidInLastMinutes("c40a9ebf1d1a242e", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))

    // the t - 2 is only valid if we go back more than 1 minute
    for (minutes <- 0 to 1) assert(!isValidInLastMinutes("432277de233d5793", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
    for (minutes <- 2 to 60) assert(isValidInLastMinutes("432277de233d5793", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))

    // the t - 3 is only valid if we go back more than 2 minutes
    for (minutes <- 0 to 2) assert(!isValidInLastMinutes("2c084d5cae6f845b", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
    for (minutes <- 3 to 60) assert(isValidInLastMinutes("2c084d5cae6f845b", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))

    // the t - 4 is only valid if we go back more than 3 minutes
    for (minutes <- 0 to 3) assert(!isValidInLastMinutes("abcf5bc1357d2139", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
    for (minutes <- 4 to 60) assert(isValidInLastMinutes("abcf5bc1357d2139", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))

    // the t - 5 is only valid if we go back more than 4 minutes
    for (minutes <- 0 to 4) assert(!isValidInLastMinutes("a67cb606af2f6a83", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
    for (minutes <- 5 to 60) assert(isValidInLastMinutes("a67cb606af2f6a83", minutes, input(prodCluster, userX, stateA), timePlus30Seconds))
  }

  it should "correctly generate the current code with hashing" in {
    assert(generateCurrent(input(prodCluster, userX, stateA), time) == "ac7da16469fe93ee")
  }

  it should "correctly detect that the same code can be used with the same attributes" in {
    assert(isValidInLastMinutes("ac7da16469fe93ee", 0, input(prodCluster, userX, stateA), time))
  }

  it should "correctly detect that the same code can't be used with a different kafka-cluster" in {
    assert(!isValidInLastMinutes("ac7da16469fe93ee", 0, input(stagingCluster, userX, stateA), time))
  }

  it should "correctly detect that the same code can't be used with a different user" in {
    assert(!isValidInLastMinutes("ac7da16469fe93ee", 0, input(prodCluster, userY, stateA), time))
  }

  it should "correctly detect that the same code can't be used with a different state" in {
    assert(!isValidInLastMinutes("ac7da16469fe93ee", 0, input(prodCluster, userX, stateB), time))
  }
}
