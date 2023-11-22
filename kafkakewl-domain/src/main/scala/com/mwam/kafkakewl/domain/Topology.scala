/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

/** The fully qualified topic id (currently the same as the topic name).
  */
final case class TopicId(value: String) extends AnyVal with StringValue
final case class TopicConfigKey(value: String) extends AnyVal with StringValue
final case class TopicConfigValue(value: String) extends AnyVal with StringValue
final case class Topic(
    name: String,
    partitions: Int = 1,
    config: Map[TopicConfigKey, TopicConfigValue] = Map.empty
) {

  /** The topic's fully qualified id is the same as the name.
    */
  def id: TopicId = TopicId(name)
}

/** The local application id in the current topology's namespace.
  */
final case class ApplicationLocalId(value: String)
    extends AnyVal
    with StringValue
final case class UserId(value: String) extends AnyVal with StringValue
final case class Application(
    id: ApplicationLocalId,
    user: UserId
    // TODO different application types
)

/** The local topic alias id in the current topology's namespace (fully
  * qualified isn't really needed anyway because aliases aren't currently
  * exposed to other topologies).
  */
final case class TopicAliasLocalId(value: String)
    extends AnyVal
    with StringValue
final case class TopicAlias(
    id: TopicAliasLocalId,
    // TODO perhaps support other ways, e.g. list of topic ids, namespace? Although all these are expressible with regex easily
    regex: String
)

/** The local application alias id in the current topology's namespace (fully
  * qualified isn't really needed anyway because aliases aren't currently
  * exposed to other topologies).
  */
final case class ApplicationAliasLocalId(value: String)
    extends AnyVal
    with StringValue
final case class ApplicationAlias(
    id: ApplicationAliasLocalId,
    // TODO perhaps support other ways, e.g. list of topic ids, namespace? Although all these are expressible with regex easily
    regex: String
)
final case class Aliases(
    topics: Seq[TopicAlias] = Seq.empty,
    applications: Seq[ApplicationAlias] = Seq.empty
)

/** ApplicationFlexId can be an application alias or application id, local or
  * fully qualified.
  */
final case class ApplicationFlexId(value: String)
    extends AnyVal
    with StringValue

/** TopicFlexId can be a local or fully qualified topic alias or fully qualified
  * topic id.
  */
final case class TopicFlexId(value: String) extends AnyVal with StringValue
final case class ProducedTopic(topic: TopicFlexId)
final case class ConsumedTopic(topic: TopicFlexId)
final case class Relationship(
    application: ApplicationFlexId,
    produce: Seq[ProducedTopic] = Seq.empty,
    consume: Seq[ConsumedTopic] = Seq.empty
)

final case class TopologyId(value: String) extends AnyVal with StringValue
final case class Namespace(value: String) extends AnyVal with StringValue
final case class Developer(value: String) extends AnyVal with StringValue
final case class Topology(
    id: TopologyId,
    namespace: Namespace,
    developers: Seq[Developer] = Seq.empty,
    topics: Seq[Topic] = Seq.empty,
    applications: Seq[Application] = Seq.empty,
    aliases: Aliases = Aliases(),
    relationships: Seq[Relationship] = Seq.empty
)

final case class TopologyDeployResult(status: String)
