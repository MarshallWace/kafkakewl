/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.topology

import com.mwam.kafkakewl.domain.IdLike

/**
  * The topology identifier (local, unique within the namespace).
  *
  * @param id the topology identifier
  */
final case class TopologyId(id: String) extends AnyVal with IdLike

/**
  * The local topic identifier in a topology (NOT fully-qualified).
  *
  * @param id the local topic identifier
  */
final case class LocalTopicId(id: String) extends AnyVal with IdLike

/**
  * The fully-qualified topic identifier.
  *
  * @param id the fully-qualified topic identifier
  */
final case class TopicId(id: String) extends AnyVal with IdLike

/**
  * The local application identifier in a topology (NOT fully-qualified).
  *
  * @param id the local application identifier
  */
final case class LocalApplicationId(id: String) extends AnyVal with IdLike

/**
  * The fully-qualified application identifier.
  *
  * @param id the fully-qualified application identifier
  */
final case class ApplicationId(id: String) extends AnyVal with IdLike

/**
  * The local alias identifier in a topology (NOT fully-qualified).
  *
  * @param id the local alias identifier
  */
final case class LocalAliasId(id: String) extends AnyVal with IdLike
object LocalAliasId {
  def apply(idLike: IdLike): LocalAliasId = new LocalAliasId(idLike.id)
}

/**
  * The fully-qualified alias identifier.
  *
  * @param id the fully-qualified alias identifier
  */
final case class AliasId(id: String) extends AnyVal with IdLike
object AliasId {
  def apply(idLike: IdLike): AliasId = new AliasId(idLike.id)
}

/**
  * The local node (application or topic) identifier (NOT fully-qualified).
  *
  * @param id the local node identifier
  */
final case class LocalNodeId(id: String) extends AnyVal with IdLike {
  def toTopicId = TopicId(id)
  def toApplicationId = ApplicationId(id)
}
object LocalNodeId {
  def apply(idLike: IdLike): LocalNodeId = new LocalNodeId(idLike.id)
}

/**
  * The fully-qualified node (application or topic) identifier.
  *
  * @param id the fully-qualified node identifier
  */
final case class NodeId(id: String) extends AnyVal with IdLike {
  def toTopicId = TopicId(id)
  def toApplicationId = ApplicationId(id)
}
object NodeId {
  def apply(idLike: IdLike): NodeId = new NodeId(idLike.id)
}

/**
  * An identifier that can reference topics/applications/aliases and can be used in relationships.
  *
  * It can be either local or fully-qualified, the node-resolution algorithm turns these into a set of actual nodes.
  *
  * @param id the string identifier
  */
final case class NodeRef(id: String) extends AnyVal with IdLike

/**
 * The replica placement description's identifier.
 *
 * @param id the replica placement description's identifier
 */
final case class ReplicaPlacementId(id: String) extends AnyVal with IdLike
