/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.utils._
import cats.syntax.either._

import scala.collection.mutable

object Expressions {
  type Variables = Map[String, Seq[String]]
  object Variables {
    def apply(v: (String, Seq[String])*): Variables = v.toMap
    def empty = Map.empty[String, Seq[String]]
  }

  type FailFast[T] = Either[Seq[String], T]

  def fromVariable(variableName: String): String = "${" + variableName + "}"

  def resolveVariables(expr: String, variables: Variables, exprName: String): FailFast[Seq[String]] = {
    // TODO it's very manual, there must be good libraries out there doing it

    sealed trait ParseState
    object ParseState {
      final case object Literal extends ParseState
      final case object EscapingNext extends ParseState
      final case object PossibleVariable extends ParseState
      final case object Variable extends ParseState
    }

    var i = 0
    var state: ParseState = ParseState.Literal
    val literalBuilder = new StringBuilder()
    val variableBuilder = new StringBuilder()
    val exprFragments = mutable.ArrayBuffer.empty[Seq[String]]
    val errors = mutable.ArrayBuffer.empty[String]
    while (i < expr.length) {
      state = (state, expr(i)) match {
        // current state: ParseState.Literal
        case (ParseState.Literal, '\\') =>
          ParseState.EscapingNext
        case (ParseState.Literal, '$') =>
          ParseState.PossibleVariable
        case (ParseState.Literal, c) =>
          literalBuilder.append(c)
          ParseState.Literal

        // current state: ParseState.EscapingNext
        case (ParseState.EscapingNext, c) =>
          literalBuilder.append(c)
          ParseState.Literal

        // current state: ParseState.PossibleVariable
        case (ParseState.PossibleVariable, '{') =>
          // finish the current literal, start a variable
          exprFragments += Seq(literalBuilder.toString)
          literalBuilder.clear()
          ParseState.Variable
        case (ParseState.PossibleVariable, '$') =>
          literalBuilder.append('$')
          ParseState.PossibleVariable
        case (ParseState.PossibleVariable, c) =>
          // if no '{' follows the '$', we just treat them as literal
          literalBuilder.append('$')
          literalBuilder.append(c)
          ParseState.Literal

        // current state: ParseState.Variable
        case (ParseState.Variable, '}') =>
          val variableName = variableBuilder.toString()
          variableBuilder.clear()
          variables.get(variableName) match {
            case Some(variableValues) => exprFragments += variableValues
            case None => errors += s"variable ${variableName.quote} is not defined in ${exprName.quote}"
          }
          ParseState.Literal
        case (ParseState.Variable, c) =>
          variableBuilder.append(c)
          ParseState.Variable
      }
      i += 1
    }

    state match {
      case ParseState.Literal | ParseState.PossibleVariable =>
        if (errors.nonEmpty) {
          errors.asLeft
        } else {
          if (state.isInstanceOf[ParseState.PossibleVariable.type]) literalBuilder.append("$")
          exprFragments += Seq(literalBuilder.toString)
          literalBuilder.clear()
          val results = exprFragments.foldLeft(Seq(""))((results, fragment) => results.flatMap(r => fragment.map(r + _)))
          results.asRight
        }

      case ParseState.EscapingNext =>
        errors += s"${exprName.quote} cannot end with an unfinished escaping character"
        errors.asLeft

      case ParseState.Variable =>
        errors += s"${exprName.quote} cannot end with an unfinished variable"
        errors.asLeft
    }
  }

  def resolveVariables[T](
    expr: String,
    variables: Variables,
    converter: String => Option[T],
    exprName: String,
    exprTypeName: String
  ): FailFast[Seq[T]] = {
    for {
      result <- resolveVariables(expr, variables, exprName)
      typedParsedResults = result.map(r => converter(r).toRight(s"expecting '$exprName' to be an $exprTypeName, but it is '$r'"))
      typedResults <- {
        val errors = typedParsedResults.collect { case Left(e) => e }
        // we fail-fast if there was any error
        Either.cond(errors.isEmpty, typedParsedResults.collect { case Right(r) => r }, errors)
      }
    } yield typedResults
  }

  def makeScalar[T](
    results: FailFast[Seq[T]],
    exprName: String
  ): FailFast[Option[T]] = {
    results.flatMap {
      case Seq() => Right(None)
      case Seq(a) => Right(Some(a))
      case s => Left(Seq(s"expecting '$exprName' to be a scalar but it is '${s.map(_.toString.quote).mkString(", ")}'"))
    }
  }

  /**
    * Base trait for expression producing zero or more results of type T.
    *
    * @tparam T the type of the result items
    */
  sealed trait Multiple[T] {
    def value(variables: Variables): FailFast[Seq[T]]
  }

  /**
    * Base trait for expression producing a single result of type T or None if there are undefined variables.
    *
    * @tparam T the type of the result
    */
  sealed trait SingleOptional[T] {
    def value(variables: Variables): FailFast[Option[T]]
  }

  /**
    * Base trait for expression producing a single result of type T.
    *
    * It either fails or produces a result. If there is no result (some variables are undefined) AND
    * there is no default value defined, it fails, otherwise falls back to the default value.
    *
    * @tparam T the type of the result
    */
  sealed trait Single[T] {
    implicit class ResultExtensions(resultWithOption: FailFast[Option[T]]) {
      def useDefault(default: T): FailFast[T] = resultWithOption.map(_.getOrElse(default))
      def failIfNone(name: String): FailFast[T] = resultWithOption.flatMap(_.toRight(Seq(s"'$name' must be defined")))
      def useDefaultOrFailIfNone(name: String, default: Option[T]): FailFast[T] =
        default.map(useDefault).getOrElse(failIfNone(name))
    }
    def value(variables: Variables): FailFast[T]
  }

  /**
    * Expression producing zero or more strings as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    */
  class MultipleStringsExpression(expr: String, name: String) extends Multiple[String] {
    def value(variables: Variables): FailFast[Seq[String]] =
      resolveVariables(expr, variables, name)
  }

  /**
    * Expression producing an generic type as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param parse the function parsing a string to the type
    * @param typeName the name of the type (for errors)
    * @param default it specifies the default value in case the expression is resolved to None, if None, it returns an error
    */
  class CustomExpression[T](expr: String, name: String, parse: String => Option[T], typeName: String, default: Option[T] = None) extends Single[T] {
    def value(variables: Variables): FailFast[T] =
      makeScalar(resolveVariables(expr, variables, parse, name, typeName), name)
        .useDefaultOrFailIfNone(name, default)
  }

  /**
    * Expression producing a generic type  or None as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param parse the function parsing a string to the type
    * @param typeName the name of the type (for errors)
    */
  class OptionalCustomExpression[T](expr: String, name: String, parse: String => Option[T], typeName: String) extends SingleOptional[T] {
    def value(variables: Variables): FailFast[Option[T]] =
      makeScalar(resolveVariables(expr, variables, parse, name, typeName), name)
  }

  /**
    * Expression producing a single string as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param default it specifies the default value in case the expression is resolved to None, if None, it returns an error
    */
  class SingleStringExpression(expr: String, name: String, default: Option[String] = None)
    extends CustomExpression[String](expr, name, Some.apply, "string", default)

  /**
    * Expression producing a single string or None as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    */
  class OptionalSingleStringExpression(expr: String, name: String)
    extends OptionalCustomExpression[String](expr, name, Some.apply, "string")

  /**
    * Expression producing a single integer as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param default it specifies the default value in case the expression is resolved to None, if None, it returns an error
    */
  class IntExpression(expr: String, name: String, default: Option[Int] = None)
    extends CustomExpression[Int](expr, name, s => s.toIntOrNone, "integer", default)

  /**
    * Expression producing a single integer or None as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    */
  class OptionalIntExpression(expr: String, name: String)
    extends OptionalCustomExpression[Int](expr, name, s => s.toIntOrNone, "integer")

  /**
    * Expression producing a single short as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param default it specifies the default value in case the expression is resolved to None, if None, it returns an error
    */
  class ShortExpression(expr: String, name: String, default: Option[Short] = None)
    extends CustomExpression[Short](expr, name, s => s.toShortOrNone, "short", default)

  /**
    * Expression producing a single short or None as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    */
  class OptionalShortExpression(expr: String, name: String)
    extends OptionalCustomExpression[Short](expr, name, s => s.toShortOrNone, "short")

  /**
    * Expression producing a single boolean as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    * @param default it specifies the default value in case the expression is resolved to None, if None, it returns an error
    */
  class BooleanExpression(expr: String, name: String, default: Option[Boolean] = None)
    extends CustomExpression[Boolean](expr, name, s => s.toBooleanOrNone, "boolean", default)

  /**
    * Expression producing a single boolean or None as the result.
    *
    * @param expr the expression
    * @param name the name of the property containing the expression (for error messages)
    */
  class OptionalBooleanExpression(expr: String, name: String)
    extends OptionalCustomExpression[Boolean](expr, name, s => s.toBooleanOrNone, "boolean")
}
