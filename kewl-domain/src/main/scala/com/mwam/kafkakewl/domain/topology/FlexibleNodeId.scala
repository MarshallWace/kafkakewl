/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import com.mwam.kafkakewl.domain

/**
  * A trait for matchers that support being associated with a topology-namespace, and returning a specific matcher for that topology namespace.
  *
  * e.g. matchers that can do local or fully-qualified matching.
  *
  * @tparam TMatcher the type of the matcher
  */
trait MatcherWithTopologyNamespace[TMatcher <: Matcher] {
  this: TMatcher =>
  def withTopologyNamespace(topologyNamespace: topology.Namespace): TMatcher = this
}

/**
  * It's like case-sensitive exact matching, but it can generate a new matcher for a topology namespace that tries local and fully-qualified matching.
  *
  * @param exact the exact string to match
  */
abstract class LocalOrFullyQualifiedExact(exact: String, topologyNamespace: Option[topology.Namespace]) extends Matcher {
  private val matcher: Matcher =
    new domain.Matcher.CompositeMatcher(
      Iterable(
        // try with exact being a local name in this topology namespace
        topologyNamespace.map(ns => new Matcher.Exact(ns.appendPart(exact).ns) {}),
        // or else fall back to exact matching (if exact is fully-qualified)
        Some(new Matcher.Exact(exact) {})
      ).flatten
    ) {}

  override def doesMatch(value: String): Boolean = matcher.doesMatch(value)
}

/**
  * A matcher for flexible d expressions in topology aliases.
  */
sealed trait FlexibleNodeId extends Matcher with MatcherWithTopologyNamespace[FlexibleNodeId] {
  /**
    * If it's true, it's expected to generate at least one matching node/relationship at the end.
    *
    * @return true if it's expected to generate at least one matching node/relationship at the end
    */
  def expectToMatch: Boolean = false
}
object FlexibleNodeId {
  final case class Exact(exact: String, topologyNamespace: Option[topology.Namespace] = None) extends LocalOrFullyQualifiedExact(exact, topologyNamespace) with FlexibleNodeId {
    override def withTopologyNamespace(topologyNamespace: topology.Namespace): FlexibleNodeId = copy(topologyNamespace = Some(topologyNamespace))
    override def expectToMatch: Boolean = true
    override def toString: String = exact // just so that if it's not matched, the error contains simply the string, not the base class's toString
  }
  final case class Regex(regex: String) extends Matcher.WholeRegex(regex) with FlexibleNodeId
  final case class Prefix(prefix: String) extends Matcher.Prefix(prefix) with FlexibleNodeId
  final case class Namespace(namespace: String) extends Matcher.Namespace(namespace) with FlexibleNodeId
}

/**
  * A matchers for topic ids in a particular topology.
  */
sealed trait FlexibleTopologyTopicId extends Matcher with MatcherWithTopologyNamespace[FlexibleTopologyTopicId]
object FlexibleTopologyTopicId {
  final case class Any() extends Matcher.Any with FlexibleTopologyTopicId
  final case class Exact(exact: String, topologyNamespace: Option[topology.Namespace] = None) extends LocalOrFullyQualifiedExact(exact, topologyNamespace) with FlexibleTopologyTopicId {
    override def withTopologyNamespace(topologyNamespace: topology.Namespace): FlexibleTopologyTopicId = copy(topologyNamespace = Some(topologyNamespace))
  }
  final case class Regex(regex: String) extends Matcher.WholeRegex(regex) with FlexibleTopologyTopicId
  final case class Prefix(prefix: String) extends Matcher.Prefix(prefix) with FlexibleTopologyTopicId
}
