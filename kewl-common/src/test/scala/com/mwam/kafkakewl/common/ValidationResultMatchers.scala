/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.common.validation.Validation.Result
import com.mwam.kafkakewl.domain.CommandError
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable

trait ValidationResultMatchers {
  case class ValidMatcher() extends Matcher[Validation.Result] {
    override def apply(left: Result): MatchResult = {
      MatchResult(left.isValid, s"$left expected to be valid", "")
    }
  }

  case class InvalidMatcher() extends Matcher[Validation.Result] {
    override def apply(left: Result): MatchResult = {
      MatchResult(left.isInvalid, s"$left expected to be invalid", "")
    }
  }

  case class InvalidContainsMessagesMatcher(messages: String*) extends Matcher[Validation.Result] {
    require(messages.nonEmpty, "messages should contain at least one expected message fragment")
    override def apply(left: Result): MatchResult = {
      val invalidContainsMessage = left
        .leftMap(f => {
          val expectedMessageFragments = mutable.Set() ++ messages.toSet
          val commandErrorsWithoutMessageFragment = mutable.ListBuffer[CommandError]()
          // for every actual command-error we must find a separate message fragment
          for (ce <- f.toList) {
            expectedMessageFragments.find(ce.message.contains) match {
              case Some(m) => expectedMessageFragments.remove(m)
              case None => commandErrorsWithoutMessageFragment.append(ce)
            }
          }
          // expecting no command-error without message fragment and no message fragment without command-error
          commandErrorsWithoutMessageFragment.isEmpty && expectedMessageFragments.isEmpty
        })
        .map(_ => false)
        .merge
      MatchResult(invalidContainsMessage, s"$left expected to be invalid and contain exactly ${messages.map(m => s"'$m'").mkString(", ")}'", "")
    }
  }

  def beValid = ValidMatcher()
  def beInvalid = InvalidMatcher()
  def containMessage(message: String) = InvalidContainsMessagesMatcher(message)
  def containMessages(messages: String*) = InvalidContainsMessagesMatcher(messages: _*)
}
