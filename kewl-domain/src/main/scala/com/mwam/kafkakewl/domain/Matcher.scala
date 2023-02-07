/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.utils._

/**
  * A matcher, that can decide if an input string matches something or not.
  */
trait Matcher {
  def doesMatch(value: String): Boolean
  def doesMatch(entityId: EntityId): Boolean = doesMatch(entityId.id)
  def doesMatch(idLike: IdLike): Boolean = doesMatch(idLike.id)
}
object Matcher {
  trait CaseIgnorable {
    val ignoreCase: Boolean
    def applyCasing(str: String): String = if (ignoreCase) str.toLowerCase else str
    def ignoreCaseString: String = if (ignoreCase) "ci" else "cs"
  }

  /**
    * Matches anything.
    */
  abstract class Any() extends Matcher {
    def doesMatch(value: String) : Boolean = true
    override def toString: String = "any"
  }

  /**
    * Matches exactly the exact string, either case-sensitively or not.
    *
    * @param exact the exact string to match
    * @param ignoreCase true if the match should be case-insensitive
    */
  abstract class Exact(exact: String, val ignoreCase: Boolean = false) extends Matcher with CaseIgnorable {
    def doesMatch(value: String) : Boolean = if (ignoreCase) value.equalsIgnoreCase(exact) else value == exact
    override def toString: String = s"""exact-$ignoreCaseString:"$exact""""
  }

  /**
    * Matches the whole string with a regex.
    *
    * @param regex the regex to use to match
    */
  abstract class WholeRegex(regex: String) extends Matcher {
    private val regexM = regex.rWhole
    def doesMatch(value: String) : Boolean = regexM.findFirstIn(value).isDefined
    override def toString: String = s"""regex:"$regexM""""
  }

  /**
    * Matches the string if it starts with the specified prefix, either case-sensitively or not.
    *
    * @param prefix the prefix to start with
    * @param ignoreCase true if the match should be case-insensitive
    */
  abstract class Prefix(prefix: String, val ignoreCase: Boolean = false) extends Matcher with CaseIgnorable {
    private val prefixToMatch = applyCasing(prefix)
    def doesMatch(value: String) : Boolean = applyCasing(value).startsWith(prefixToMatch)
    override def toString: String = s"""prefix-$ignoreCaseString:"$prefix""""
  }

  /**
    * Matches the string if it's in the specified namespace, either case-sensitively or not.
    *
    * @param namespace the namespace
    * @param ignoreCase true if the match should be case-insensitive
    */
  abstract class Namespace(namespace: String, val ignoreCase: Boolean = false) extends Matcher with CaseIgnorable {
    private val namespaceToMatch = applyCasing(namespace)
    def doesMatch(value: String) : Boolean = {
      val valueToMatch = applyCasing(value)
      namespaceToMatch.isEmpty || valueToMatch == namespaceToMatch || valueToMatch.startsWith(namespaceToMatch + ".")
    }
    override def toString: String = s"""namespace-$ignoreCaseString:"$namespace""""
  }

  /**
    * Matches all the specified matchers, if any of them returns true (the first one), it returns true.
    *
    * @param matchers the matchers to use
    */
  abstract class CompositeMatcher(matchers: Iterable[Matcher]) extends Matcher  {
    def doesMatch(value: String): Boolean = matchers.exists(_.doesMatch(value))
  }

  implicit class MatchersExtensions[TMatcher <: Matcher](matchers: Iterable[TMatcher]) {
    def doesMatch(value: String): Boolean = matchers.exists(_.doesMatch(value))
    def doesMatch(entityId: EntityId): Boolean = doesMatch(entityId.id)
    def doesMatch(idLike: IdLike): Boolean = doesMatch(idLike.id)

    def doesMatchMatchers(value: String): Iterable[TMatcher] = matchers.filter(_.doesMatch(value))
    def doesMatchMatchers(entityId: EntityId): Iterable[TMatcher] = doesMatchMatchers(entityId.id)
    def doesMatchMatchers(idLike: IdLike): Iterable[TMatcher] = doesMatchMatchers(idLike.id)
  }
}
