/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

final case class TopologyId(value: String) extends AnyVal with StringValue
final case class Namespace(value: String) extends AnyVal with StringValue
final case class TopicId(value: String) extends AnyVal with StringValue
final case class TopicConfigKey(value: String) extends AnyVal with StringValue
final case class TopicConfigValue(value: String) extends AnyVal with StringValue

final case class Topic(
  name: String,
  partitions: Int,
  config: Map[TopicConfigKey, TopicConfigValue]
)
final case class Topology(
  id: TopologyId,
  namespace: Namespace,
  topics: Map[TopicId, Topic]
)

object Topology
{
}

final case class TopologyDeployResult(status: String)

