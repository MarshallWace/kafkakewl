/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import java.time._
import java.time.temporal.ChronoUnit

import cats.data.NonEmptyList
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId

/**
  * Functionality to generate codes for a user and a particular time.
  */
object AuthorizationCode {
  final case class Input(value: String) extends AnyVal {
    override def toString: String = value
  }

  object Generator {
    sealed trait TimeBy {
      def truncate(time: LocalDateTime): LocalDateTime
      def minus(time: LocalDateTime, n: Int): LocalDateTime
      def minusTruncate(time: LocalDateTime, n: Int): LocalDateTime = truncate(minus(time, n))
    }
    object TimeBy {
      final case object Minutes extends TimeBy {
        def truncate(time: LocalDateTime): LocalDateTime = time.truncatedTo(ChronoUnit.MINUTES)
        def minus(time: LocalDateTime, n: Int): LocalDateTime = time.minusMinutes(n)
      }
    }

    type HashFunc = String => String
    type GenerateFunc = (Input, LocalDateTime, Int) => NonEmptyList[String]

    val sha256: HashFunc = { input =>
      java.security.MessageDigest.getInstance("SHA-256")
        .digest(input.getBytes("UTF-8"))
        .map("%02x".format(_)).mkString
    }

    val sha256_truncated_16: HashFunc = sha256 andThen (_.substring(0, 16))

    def generate(
      input: Input,
      time: LocalDateTime = LocalDateTime.now(Clock.systemUTC()),
      numberOfPreviousResults: Int = 0,
      timeBy: TimeBy = TimeBy.Minutes,
      hashFunc: HashFunc = sha256_truncated_16
    ): NonEmptyList[String] = {
      val inputFrom: Int => String = m => s"$input: ${timeBy.minusTruncate(time, m)}"
      NonEmptyList.fromListUnsafe(
        (0 to numberOfPreviousResults)
          .map(inputFrom andThen hashFunc)
          .toList
      )
    }

    val generateByMinutesSha256Truncated10: GenerateFunc = Generator.generate(_, _, _, Generator.TimeBy.Minutes, Generator.sha256_truncated_16)

    def inputFor(
      kafkaClusterId: KafkaClusterEntityId,
      userName: String,
      additionalState: String
    ) = Input(s"$kafkaClusterId/$userName[$additionalState]")
  }

  def isValidInLastMinutes(
    code: String,
    minutes: Int,
    input: Input,
    time: LocalDateTime = LocalDateTime.now(Clock.systemUTC()),
    generateFunc: Generator.GenerateFunc = Generator.generateByMinutesSha256Truncated10
  ): Boolean =
    generateFunc(input, time, minutes).toList.contains(code)

  def generateCurrent(
    input: Input,
    time: LocalDateTime = LocalDateTime.now(Clock.systemUTC()),
    generateFunc: Generator.GenerateFunc = Generator.generateByMinutesSha256Truncated10
  ): String = generateFunc(input, time, 0).head
}
