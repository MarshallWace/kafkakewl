/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafka.config

import cats.syntax.option._

/**
 * The default for a particular topic config
 *
 * @param overridable true if it can be overriden in the topic
 * @param default the default value if it's not specified in the topic - if empty it indicates that the config shouldn't be set
 */
final case class TopicConfigDefault(overridable: Boolean, default: Option[String] = None)

object TopicConfigDefault {
  def noReplicaPlacement(overridable: Boolean = false): (String, TopicConfigDefault) =
    TopicConfigKeys.confluentPlacementConstraints -> TopicConfigDefault(overridable, none)

  def fromReplicaPlacement(replicaPlacement: String): (String, TopicConfigDefault) =
    TopicConfigKeys.confluentPlacementConstraints -> TopicConfigDefault(overridable = false, replicaPlacement.some)

  def fromMinInsyncReplicas(minInsyncReplicas: Short, overridable: Boolean = false): (String, TopicConfigDefault) =
    TopicConfigKeys.minInsyncReplicas -> TopicConfigDefault(overridable, minInsyncReplicas.toString.some)

  def noMinInsyncReplicas(overridable: Boolean = false): (String, TopicConfigDefault) =
    TopicConfigKeys.minInsyncReplicas -> TopicConfigDefault(overridable, none)
}
