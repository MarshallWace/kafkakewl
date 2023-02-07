/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

/**
  * The relationship types.
  */
sealed trait RelationshipType {
  def isConsume: Boolean = isInstanceOf[RelationshipType.Consume]
  def isProduce: Boolean = isInstanceOf[RelationshipType.Produce]
  def isConsumeOrProduce: Boolean = isConsume || isProduce
  def isCustom: Boolean = isInstanceOf[RelationshipType.Custom]
}
sealed trait ConsumeProduceRelationshipType extends RelationshipType
object RelationshipType {
  final case class Consume() extends ConsumeProduceRelationshipType { override def toString: String = "consume" }
  final case class Produce() extends ConsumeProduceRelationshipType { override def toString: String = "produce" }
  final case class Custom(relationship: String) extends RelationshipType  { override def toString: String = relationship.toLowerCase }

  def nonCustom: Seq[String] = Seq(Consume().toString, Produce().toString)

  def apply(r: String): RelationshipType = r.toLowerCase match {
    case "consume" => RelationshipType.Consume()
    case "produce" => RelationshipType.Produce()
    case _ => RelationshipType.Custom(r)
  }
}

/**
  * The possible relationship reset-modes.
  */
sealed trait ResetMode
object ResetMode {
  final case object Ignore extends ResetMode
  final case object Beginning extends ResetMode
  final case object End extends ResetMode
}

/**
  * Developers' access to the topology's topics.
  */
sealed trait DevelopersAccess
object DevelopersAccess {
  final case object Full extends DevelopersAccess
  final case object TopicReadOnly extends DevelopersAccess
//  final case object FullExactTopics extends DevelopersAccess
//  final case object TopicReadOnlyExactTopics extends DevelopersAccess

  def parse(input: String): Option[DevelopersAccess] = input.toLowerCase match {
    case "full" => Some(Full)
    case "topicreadonly" => Some(TopicReadOnly)
//    case "fullexacttopics" | "fullexact" => Some(FullExactTopics)
//    case "topicreadonlyexacttopics" | "topicreadonlyexact" => Some(TopicReadOnlyExactTopics)
    case _ => None
  }
}

/**
  * Aliases (local node id)
  */
object LocalAliases {
  type LocalAliasesMap = Map[LocalAliasId, Seq[FlexibleNodeId]]
  object LocalAliasesMap {
    val empty: LocalAliasesMap = Map.empty
  }
}
final case class LocalAliases(
  topics: LocalAliases.LocalAliasesMap = LocalAliases.LocalAliasesMap.empty,
  applications: LocalAliases.LocalAliasesMap = LocalAliases.LocalAliasesMap.empty
)

/**
  * Aliases (fully-qualified id)
  */
object Aliases {
  type AliasesMap = Map[AliasId, Seq[FlexibleNodeId]]
  object AliasesMap {
    val empty: AliasesMap = Map.empty
  }
}
final case class Aliases(
  topics: Aliases.AliasesMap = Aliases.AliasesMap.empty,
  applications: Aliases.AliasesMap = Aliases.AliasesMap.empty
)
