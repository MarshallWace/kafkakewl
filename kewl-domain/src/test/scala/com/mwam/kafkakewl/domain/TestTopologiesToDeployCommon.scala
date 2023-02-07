/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.topology._

final case class TopologyToDeployTopicWithId(topicId: LocalTopicId, topic: TopologyToDeploy.Topic) {
  def toTuple: (LocalTopicId, TopologyToDeploy.Topic) = (topicId, topic)
}

final case class TopologyToDeployApplicationWithId(applicationId: LocalApplicationId, application: TopologyToDeploy.Application) {
  def toTuple: (LocalApplicationId, TopologyToDeploy.Application) = (applicationId, application)
}

trait TestTopologiesToDeployCommon {
  implicit def topologyToDeployTopicWithIdToTuple(topicWithId: TopologyToDeployTopicWithId): (LocalTopicId, TopologyToDeploy.Topic) = topicWithId.toTuple
  implicit def topologyToDeployApplicationWithIdToTuple(applicationWithId: TopologyToDeployApplicationWithId): (LocalApplicationId, TopologyToDeploy.Application) = applicationWithId.toTuple

  implicit class TopologyToDeployTopicIdExtensions(topicId: LocalTopicId) {
    def --> (topic: TopologyToDeploy.Topic) = TopologyToDeployTopicWithId(topicId, topic)
  }
  implicit class TopologyToDeployApplicationIdExtensions(applicationId: LocalApplicationId) {
    def --> (application: TopologyToDeploy.Application) = TopologyToDeployApplicationWithId(applicationId, application)
  }

  implicit class TopologyToDeployExtensions(topology: TopologyToDeploy) {
    // removing the given topic/application/relationship
    def withoutTopic(topicId: LocalTopicId): TopologyToDeploy = topology.copy(topics = topology.topics - topicId)
    def withoutApplication(applicationId: LocalApplicationId): TopologyToDeploy = topology.copy(applications = topology.applications.filterKeys(_ != applicationId))

    def withoutRelationshipOf(nodeRef: NodeRef): TopologyToDeploy =
      topology.copy(relationships = topology.relationships - nodeRef)
    def withRelationship(relationship: (NodeRef, TopologyToDeploy.NodeRelationships)): TopologyToDeploy = topology.copy(relationships = topology.relationships + relationship)

    // getting the given topic/application/relationship
    def topic(topicId: String): Option[TopologyToDeploy.Topic] = topology.topics.get(LocalTopicId(topicId))

    // adding the given topic/application/relationship
    def + (idAndTopic: TopologyToDeployTopicWithId): TopologyToDeploy = topology.copy(topics = topology.topics + idAndTopic.toTuple)
    def + (idAndApplication: TopologyToDeployApplicationWithId): TopologyToDeploy = topology.copy(applications = topology.applications + idAndApplication.toTuple)
    def + (aliases: LocalAliases): TopologyToDeploy = topology.copy(aliases = LocalAliases(topology.aliases.topics ++ aliases.topics, topology.aliases.applications ++ aliases.applications))

    // replacing the given topic/application/relationship
    def ~+ (idAndTopic: TopologyToDeployTopicWithId): TopologyToDeploy = topology.withoutTopic(idAndTopic.topicId) + idAndTopic
    def ~+ (idAndApplication: TopologyToDeployApplicationWithId): TopologyToDeploy = topology.withoutApplication(idAndApplication.applicationId) + idAndApplication

    def updateApplication(applicationId: String, updateFunc: TopologyToDeploy.Application => TopologyToDeploy.Application): TopologyToDeploy = {
      topology.copy(
        applications = topology.applications.map {
          case (id, app) => (id, if (id.id == applicationId) updateFunc(app) else app)
        }
      )
    }

    def updateTopic(topicId: String, updateFunc: TopologyToDeploy.Topic => TopologyToDeploy.Topic): TopologyToDeploy = {
      topology.copy(
        topics = topology.topics.map {
          case (id, topic) => (id, if (id.id == topicId) updateFunc(topic) else topic)
        }
      )
    }

    def updateApplications(applicationIds: Set[String], updateFunc: TopologyToDeploy.Application => TopologyToDeploy.Application): TopologyToDeploy = {
      topology.copy(
        applications = topology.applications.map {
          case (id, app) => (id, if (applicationIds(id.id)) updateFunc(app) else app)
        }
      )
    }
  }

  implicit class ApplicationExtensions(application: TopologyToDeploy.Application) {
    def makeSimple(consumerGroup: Option[String] = None, transactionalId: Option[String] = None): TopologyToDeploy.Application =
      application.copy(`type` = TopologyToDeploy.Application.Type.Simple(consumerGroup, transactionalId))
    def makeKafkaStreams(kafkaStreamsAppId: String): TopologyToDeploy.Application =
      application.copy(`type` = TopologyToDeploy.Application.Type.KafkaStreams(kafkaStreamsAppId))
    def makeConnector(connector: String): TopologyToDeploy.Application =
      application.copy(`type` = TopologyToDeploy.Application.Type.Connector(connector))
    def makeConnectReplicator(connectReplicator: String): TopologyToDeploy.Application =
      application.copy(`type` = TopologyToDeploy.Application.Type.ConnectReplicator(connectReplicator))
  }

  def toDeployRelationshipFrom(
    nodeRef: String,
    relationships: (RelationshipType, Seq[(String, Option[TopologyToDeploy.RelationshipProperties])])*
  ): (NodeRef, Map[RelationshipType, Seq[TopologyToDeploy.NodeRelationshipProperties]]) = {
    (
      NodeRef(nodeRef),
      relationships
        .map { case (relType, rels) =>
          (relType, rels.map { case (nid, rp) => TopologyToDeploy.NodeRelationshipProperties(NodeRef(nid), rp.getOrElse(TopologyToDeploy.RelationshipProperties())) })
        }
        .toMap
    )
  }

  implicit class TopologySeqExtensions(topologies: Seq[TopologyToDeploy]) {
    def toMapByTopologyId: Map[TopologyEntityId, TopologyToDeploy] = topologies.map(t => (t.topologyEntityId, t)).toMap
  }

  implicit class TopicMapExtensions(topics: Map[String, TopologyToDeploy.Topic]) {
    def toMapByTopicId: Map[LocalTopicId, TopologyToDeploy.Topic] = topics.map { case (k, a) => (LocalTopicId(k), a) }
  }

  implicit class ApplicationMapExtensions(applications: Map[String, TopologyToDeploy.Application]) {
    def toMapByApplicationId: Map[LocalApplicationId, TopologyToDeploy.Application] = applications.map { case (k, a) => (LocalApplicationId(k), a) }
  }
}
