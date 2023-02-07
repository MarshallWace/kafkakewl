/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafkacluster

import com.mwam.kafkakewl.domain.Matcher

sealed trait TopicConfigKeyConstraint extends Matcher
object TopicConfigKeyConstraint {
  final case class Exact(exact: String) extends Matcher.Exact(exact) with TopicConfigKeyConstraint
  final case class Regex(regex: String) extends Matcher.WholeRegex(regex) with TopicConfigKeyConstraint
  final case class Prefix(prefix: String) extends Matcher.Prefix(prefix) with TopicConfigKeyConstraint
}

trait TopicConfigKeyConstraintBase {
  val include: Seq[TopicConfigKeyConstraint]
  val exclude: Seq[TopicConfigKeyConstraint]

  protected def includeContains(topicConfigKey: String): Boolean = include.exists(_.doesMatch(topicConfigKey))
  protected def excludeContains(topicConfigKey: String): Boolean = exclude.exists(_.doesMatch(topicConfigKey))

  def isIncluded(topicConfigKey: String): Boolean
}

/**
 * An TopicConfigKeyConstraintBase implementation that by default includes everything (even if include is empty)
 */
final case class TopicConfigKeyConstraintInclusive(
  include: Seq[TopicConfigKeyConstraint] = Seq.empty,
  exclude: Seq[TopicConfigKeyConstraint] = Seq.empty
) extends TopicConfigKeyConstraintBase {
  /**
   * Returns true if the specified topic config key is included - in the include-list (or the include-list is empty) AND not in the exclude-list.
   *
   * @param topicConfigKey the topic config key to check
   */
  def isIncluded(topicConfigKey: String): Boolean = (include.isEmpty || includeContains(topicConfigKey)) && !excludeContains(topicConfigKey)
}

/**
 * An TopicConfigKeyConstraintBase implementation that by default excludes everything.
 */
final case class TopicConfigKeyConstraintExclusive(
  include: Seq[TopicConfigKeyConstraint] = Seq.empty,
  exclude: Seq[TopicConfigKeyConstraint] = Seq.empty
) extends TopicConfigKeyConstraintBase {
  /**
   * Returns true if the specified topic config key is included - in the include-list AND not in the exclude-list.
   *
   * @param topicConfigKey the topic config key to check
   */
  def isIncluded(topicConfigKey: String): Boolean = includeContains(topicConfigKey) && !excludeContains(topicConfigKey)
}
