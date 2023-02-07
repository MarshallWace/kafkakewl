/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import cats.data.ValidatedNel
import com.mwam.kafkakewl.domain.CommandError
import com.mwam.kafkakewl.domain.topology.JsonEncodeDecode._
import com.mwam.kafkakewl.domain.topology.TopologyToDeploy
import io.circe.syntax._
import org.scalatest.matchers.{MatchResult, Matcher}

trait MakeDeployableMatchers {
  case class Success(topology: TopologyToDeploy) extends Matcher[ValidatedNel[CommandError, TopologyToDeploy]] {
    override def apply(left: ValidatedNel[CommandError, TopologyToDeploy]): MatchResult = {
      MatchResult(left.isValid && left.exists(_ == topology), s"${left.map(_.asJson.spaces2).getOrElse(left)}\nexpected to generate\n${topology.asJson.spaces2}", "")
    }
  }

  case class Failure(errorMessageFragment: String) extends Matcher[ValidatedNel[CommandError, TopologyToDeploy]] {
    override def apply(left: ValidatedNel[CommandError, TopologyToDeploy]): MatchResult = {
      MatchResult(left.isInvalid && left.toEither.left.exists(e => e.exists(_.message.contains(errorMessageFragment))), s"${left.map(_.asJson.spaces2).getOrElse(left)}\nexpected to fail with '$errorMessageFragment'", "")
    }
  }

  def makeDeployable(topology: TopologyToDeploy): Success = Success(topology)
  def containsError(errorMessageFragment: String): Failure = Failure(errorMessageFragment)
}
