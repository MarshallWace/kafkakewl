/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import cats.syntax.option._
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigDefault
import com.mwam.kafkakewl.domain.topology.ReplicaPlacementId

package object kafkacluster {
  /**
   * The replica-placement config is effectively a set of topic config defaults.
   */
  type ReplicaPlacementConfig = Map[String, TopicConfigDefault]

  /**
   * All the replica-placements keyed by the replica-placement-id.
   */
  type ReplicaPlacements = Map[ReplicaPlacementId, ReplicaPlacementConfig]

  /**
   * The empty replica-placements as a default for the kafka-clusters.
   */
  val emptyReplicaPlacements: Map[ReplicaPlacementId, ReplicaPlacementConfig] = Map.empty

  type ResolveTopicConfig = (Option[ReplicaPlacementId], Map[String, String]) => Map[String, String]
  val nullResolveReplicaPlacement: ResolveTopicConfig = (_, topicConfig) => topicConfig

  /**
   * Resolves the actual topic config from the config specified in the topology and the defaults.
   *
   * @param topicConfig the topic config in the topology
   * @param topicConfigDefaults the defaults
   * @return the resolved topic config
   */
  def applyTopicConfigDefaults(
    topicConfig: Map[String, String],
    topicConfigDefaults: Map[String, TopicConfigDefault]
  ): Map[String, String] = {
    // these are the default topic configs that can be overridden in the topic
    val overridableDefaults = topicConfigDefaults.collect { case (key, TopicConfigDefault(true, default)) => (key, default) }
    // these are the topic configs that cannot be overridden in the topic (in fact it's a validation error to set them in the topic when these topic config defaults are chosen)
    val overridingConfigs = topicConfigDefaults.collect { case (key, TopicConfigDefault(false, default)) => (key, default) }

    val resolvedTopicConfig = overridableDefaults ++ topicConfig.mapValues(_.some) ++ overridingConfigs
    // filtering out any empty config, those simply shouldn't be set
    resolvedTopicConfig
      .collect { case (key, Some(value)) => (key, value) }
      .filter { case (_, value) => value != null && value.nonEmpty }
  }

  type IsTopicConfigManaged = String => Boolean
  val allTopicConfigsAreManaged: IsTopicConfigManaged = _ => true

  type IsTopicConfigValueEquivalent = (String, String, String) => Boolean
  val sameTopicConfigAreEquivalent: IsTopicConfigValueEquivalent = (_, desiredValue, actualValue) => desiredValue == actualValue
  /**
   * Replica-placement constraints can have a list of equivalent constraints that we consider equal. E.g. if we set
   * the replica-placement constraint to X when the topic is created, but X has a list of equivalents: [Y, Z], then
   * we won't update the topic config even if we find that the kafka cluster has Y or Z as replica-placement.
   */
  type ReplicaPlacementConfigEquivalents = Map[String, Seq[String]]
  val emptyReplicaPlacementConfigEquivalents: ReplicaPlacementConfigEquivalents = Map.empty
}
