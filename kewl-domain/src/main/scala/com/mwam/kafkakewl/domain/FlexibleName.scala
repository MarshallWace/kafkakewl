/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

/**
  * It's a flexible name that can match names in different ways.
  *
  * This one is used in generic cases, e.g. for allowing namespaces to consume/produce topics/applications, etc..
  */
sealed trait FlexibleName extends Matcher
object FlexibleName {
  final case class Any() extends Matcher.Any with FlexibleName
  final case class Exact(exact: String) extends Matcher.Exact(exact) with FlexibleName
  final case class Regex(regex: String) extends Matcher.WholeRegex(regex) with FlexibleName
  final case class Prefix(prefix: String) extends Matcher.Prefix(prefix) with FlexibleName
  final case class Namespace(namespace: String) extends Matcher.Namespace(namespace) with FlexibleName

  def decodeFromJson(flexibleName: String): Either[io.circe.Error, Seq[FlexibleName]] = {
    import com.mwam.kafkakewl.domain.JsonEncodeDecodeBase._
    import io.circe.parser.decode
    decode[Seq[FlexibleName]](flexibleName)
  }
}
