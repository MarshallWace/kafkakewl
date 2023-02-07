/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import com.mwam.kafkakewl.utils._

/**
  * A namespace string (fragments separated with dots).
  *
  * @param ns the namespace string
  */
final case class Namespace(ns: String) extends AnyVal {
  override def toString: String = ns
  def isEmpty: Boolean = ns.trim.isEmpty
  def quote: String = ns.quote

  def contains(fullyQualifiedName: String): Boolean = ns.isEmpty || fullyQualifiedName.startsWith(ns + ".")

  def appendPart(part: String): Namespace = {
    if (isEmpty && part.isEmpty)
      this
    else if (isEmpty)
      Namespace(part)
    else if (part.isEmpty)
      this
    else
      Namespace(ns + "." + part)
  }

  def appendIdLike(idLike: IdLike): String = appendPart(idLike.id).ns
  def appendNodeId(nodeId: LocalNodeId): NodeId = NodeId(appendIdLike(nodeId))
  def appendTopicId(topicId: LocalTopicId): TopicId = TopicId(appendIdLike(topicId))
  def appendApplicationId(applicationId: LocalApplicationId): ApplicationId = ApplicationId(appendIdLike(applicationId))
  def appendAliasId(aliasId: LocalAliasId): AliasId = AliasId(appendIdLike(aliasId))
}
object Namespace {
  def apply(namespace: Namespace, topologyId: TopologyId): Namespace = namespace.appendPart(topologyId.id)
}
