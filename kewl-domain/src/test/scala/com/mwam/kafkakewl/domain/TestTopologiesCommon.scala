/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.topology._

final case class TopologyTopicWithId(topicId: LocalTopicId, topic: Topology.Topic) {
  def toTuple: (LocalTopicId, Topology.Topic) = (topicId, topic)
}

final case class TopologyApplicationWithId(applicationId: LocalApplicationId, application: Topology.Application) {
  def toTuple: (LocalApplicationId, Topology.Application) = (applicationId, application)
}

trait TestTopologiesCommon {
  implicit def topologyTopicWithIdToTuple(topicWithId: TopologyTopicWithId): (LocalTopicId, Topology.Topic) = topicWithId.toTuple
  implicit def topologyApplicationWithIdToTuple(applicationWithId: TopologyApplicationWithId): (LocalApplicationId, Topology.Application) = applicationWithId.toTuple

  implicit class TopologyTopicIdExtensions(topicId: LocalTopicId) {
    def --> (topic: Topology.Topic) = TopologyTopicWithId(topicId, topic)
  }
  implicit class TopologyApplicationIdExtensions(applicationId: LocalApplicationId) {
    def --> (application: Topology.Application) = TopologyApplicationWithId(applicationId, application)
  }

  // implicit conversions to make creating test-topologies easier
  import scala.language.implicitConversions
  implicit def StringToTopologyUserExp(str: String): Topology.UserExpr = Topology.UserExpr(str)
  implicit def StringToPartitionsExpr(str: String): Topology.PartitionsExpr = Topology.PartitionsExpr(str)
  implicit def StringToTopicConfigExpr(str: String): Topology.TopicConfigExpr = Topology.TopicConfigExpr(str)
  implicit def StringToTagExpr(str: String): Topology.TagExpr = Topology.TagExpr(str)
  implicit def StringToLabelKeyExpr(str: String): Topology.LabelKeyExpr = Topology.LabelKeyExpr(str)
  implicit def StringToLabelValueExpr(str: String): Topology.LabelValueExpr = Topology.LabelValueExpr(str)
  implicit def StringStringMapToLabelsExpr(m: Map[String, String]): Topology.LabelsExpr = m.map { case (k, v) => (Topology.LabelKeyExpr(k), Topology.LabelValueExpr(v)) }

  implicit class TopologyExtensions(topology: Topology) {
    // removing the given topic/application/relationship
    def withoutTopic(topicId: LocalTopicId): Topology = topology.copy(topics = topology.topics - topicId)
    def withoutApplication(applicationId: LocalApplicationId): Topology = topology.copy(applications = topology.applications - applicationId)

    def withoutRelationshipOf(nodeRef: NodeRef): Topology =
      topology.copy(relationships = topology.relationships - nodeRef)
    def withRelationship(relationship: (NodeRef, Topology.NodeRelationships)): Topology = topology.copy(relationships = topology.relationships + relationship)

    // adding the given topic/application/relationship
    def + (idAndTopic: TopologyTopicWithId): Topology = topology.copy(topics = topology.topics + idAndTopic.toTuple)
    def + (idAndApplication: TopologyApplicationWithId): Topology = topology.copy(applications = topology.applications + idAndApplication.toTuple)
    def + (aliases: LocalAliases): Topology = topology.copy(aliases = LocalAliases(topology.aliases.topics ++ aliases.topics, topology.aliases.applications ++ aliases.applications))

    // replacing the given topic/application/relationship
    def ~+ (idAndTopic: TopologyTopicWithId): Topology = topology.withoutTopic(idAndTopic.topicId) + idAndTopic
    def ~+ (idAndApplication: TopologyApplicationWithId): Topology = topology.withoutApplication(idAndApplication.applicationId) + idAndApplication

    def updateApplication(applicationId: String, updateFunc: Topology.Application => Topology.Application): Topology = {
      topology.copy(
        applications = topology.applications.map {
          case (id, app) => (id, if (id.id == applicationId) updateFunc(app) else app)
        }
      )
    }

    def updateTopic(topicId: String, updateFunc: Topology.Topic => Topology.Topic): Topology = {
      topology.copy(
        topics = topology.topics.map {
          case (id, topic) => (id, if (id.id == topicId) updateFunc(topic) else topic)
        }
      )
    }

    def updateApplications(applicationIds: Set[String], updateFunc: Topology.Application => Topology.Application): Topology = {
      topology.copy(
        applications = topology.applications.map {
          case (id, app) => (id, if (applicationIds(id.id)) updateFunc(app) else app)
        }
      )
    }
  }

  implicit class ApplicationExtensions(application: Topology.Application) {
    def makeSimple(consumerGroup: Option[String] = None, transactionalId: Option[String] = None): Topology.Application =
      application.copy(`type` = Topology.Application.Type.Simple(consumerGroup, transactionalId))
    def makeKafkaStreams(kafkaStreamsAppId: String): Topology.Application =
      application.copy(`type` = Topology.Application.Type.KafkaStreams(kafkaStreamsAppId))
    def makeConnector(connector: String): Topology.Application =
      application.copy(`type` = Topology.Application.Type.Connector(connector))
    def makeConnectReplicator(connectReplicator: String): Topology.Application =
      application.copy(`type` = Topology.Application.Type.ConnectReplicator(connectReplicator))
  }

  def relationshipFrom(
    nodeRef: String,
    relationships: (RelationshipType, Seq[(String, Option[Topology.RelationshipProperties])])*
  ): (NodeRef, Map[RelationshipType, Seq[Topology.NodeRelationshipProperties]]) = {
    (
      NodeRef(nodeRef),
      relationships
        .map { case (relType, rels) =>
          (relType, rels.map { case (nid, rp) => Topology.NodeRelationshipProperties(NodeRef(nid), rp.getOrElse(Topology.RelationshipProperties())) })
        }
        .toMap
    )
  }

  implicit class TopologySeqExtensions(topologies: Seq[Topology]) {
    def toMapByTopologyId: Map[TopologyEntityId, Topology] = topologies.map(t => (t.topologyEntityId, t)).toMap
  }

  implicit class TopicMapExtensions(topics: Map[String, Topology.Topic]) {
    def toMapByTopicId: Map[LocalTopicId, Topology.Topic] = topics.map { case (k, a) => (LocalTopicId(k), a) }
  }

  implicit class ApplicationMapExtensions(applications: Map[String, Topology.Application]) {
    def toMapByApplicationId: Map[LocalApplicationId, Topology.Application] = applications.map { case (k, a) => (LocalApplicationId(k), a) }
  }

  implicit class AliasMapSingleExtensions(aliases: Map[String, FlexibleNodeId]) {
    def toAliases: LocalAliases.LocalAliasesMap = aliases.map { case (nid, fn) => (LocalAliasId(nid), Seq(fn)) }
  }

  implicit class AliasMapMultipleExtensions(aliases: Map[String, Seq[FlexibleNodeId]]) {
    def toAliases: LocalAliases.LocalAliasesMap = aliases.map { case (nid, fn) => (LocalAliasId(nid), fn) }
  }
}
