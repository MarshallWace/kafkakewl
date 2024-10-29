/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.migrate

import cats.syntax.option._
import com.mwam.kafkakewl.common.topology.TopologyLikeOperations
import com.mwam.kafkakewl.domain.FlexibleName
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyToDeploy.NodeRelationships
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.LazyLogging
import io.circe.Json

sealed trait DeployedTopologyMigrateResult
object DeployedTopologyMigrateResult {
  final case class Deploy(topology: Json) extends DeployedTopologyMigrateResult
  final case class Delete(topologyId: String) extends DeployedTopologyMigrateResult

}

object DeployedTopologyMigrate {
  def deploy(
    deployedTopologies: Map[DeployedTopologyEntityId, DeployedTopology],
    deployedTopology: DeployedTopology
  ): Option[DeployedTopologyMigrateResult] = {
    if (shouldMigrate(deployedTopology)) {
      val migrate = new MigrateImpl(deployedTopologies, deployedTopology)
      migrate.migrateDeployedTopology().some
    } else {
      none
    }
  }

  def remove(
    deployedTopologies: Map[DeployedTopologyEntityId, DeployedTopology],
    deployedTopology: DeployedTopology
  ): Option[DeployedTopologyMigrateResult.Delete] = {
    if (shouldMigrate(deployedTopology)) {
      val migrate = new MigrateImpl(deployedTopologies, deployedTopology)
      migrate.migrateRemovedDeployedTopology().some
    } else {
      none
    }
  }

  private def shouldMigrate(deployedTopology: DeployedTopology): Boolean = {
    val topologyId = deployedTopology.topologyId.id
    val kafkaClusterId = deployedTopology.kafkaClusterId.id
    val shouldMigrateTopology = true
    val shouldMigrateKafkaCluster = true
    shouldMigrateTopology && shouldMigrateKafkaCluster
  }

  sealed trait TopologyNodeDetails {
    val topology: TopologyToDeploy
    val localId: LocalNodeId
  }
  object TopologyNodeDetails {
    final case class Topic(topology: TopologyToDeploy, LocalTopicId: LocalTopicId, topic: TopologyToDeploy.Topic) extends TopologyNodeDetails {
      override val localId: LocalNodeId = LocalNodeId(LocalTopicId)
    }
    final case class Application(topology: TopologyToDeploy, LocalApplicationId: LocalApplicationId, application: TopologyToDeploy.Application) extends TopologyNodeDetails {
      override val localId: LocalNodeId = LocalNodeId(LocalApplicationId)
      lazy val FullyQualifiedIdVnext: String = topology.namespace.appendApplicationId(LocalApplicationId).id
    }
    final case class TopicAlias(topology: TopologyToDeploy, LocalTopicAliasId: LocalAliasId, topicAlias: Seq[FlexibleNodeId]) extends TopologyNodeDetails {
      override val localId: LocalNodeId = LocalNodeId(LocalTopicAliasId)
    }
    final case class ApplicationAlias(topology: TopologyToDeploy, LocalApplicationAliasId: LocalAliasId, applicationAlias: Seq[FlexibleNodeId]) extends TopologyNodeDetails {
      override val localId: LocalNodeId = LocalNodeId(LocalApplicationAliasId)
    }
  }

  private class MigrateImpl(
    private val deployedTopologies: Map[DeployedTopologyEntityId, DeployedTopology],
    private val deployedTopology: DeployedTopology,
  ) extends LazyLogging with MdcUtils
    // For useful utilities for looking up NodeRefs
    with TopologyLikeOperations[TopologyToDeploy.Node, TopologyToDeploy.Topic, TopologyToDeploy.Application, TopologyToDeploy.RelationshipProperties] {

    private lazy val topologyById = calculateTopologyById()
    private lazy val topologyByIdWithCurrent = calculateTopologyById(deployedTopology.some)

    private lazy val topicDetailsById = calculateTopicDetailsById(topologyById.values)
    private lazy val applicationDetailsById = calculateApplicationDetailsById(topologyById.values)
    private lazy val topicDetailsByIdWithCurrent = calculateTopicDetailsById(topologyByIdWithCurrent.values)
    private lazy val applicationDetailsByIdWithCurrent = calculateApplicationDetailsById(topologyByIdWithCurrent.values)

    private lazy val topologyToDeployOption = deployedTopology.topologyWithVersion.map(_.topology)

    private def jsonBooleanO(b: Option[Boolean]): Json = b.map(Json.fromBoolean).getOrElse(Json.Null)
    private def jsonStringO(str: Option[String]): Json = str.map(Json.fromString).getOrElse(Json.Null)
    private def jsonIntO(i: Option[Int]): Json = i.map(Json.fromInt).getOrElse(Json.Null)
    private def jsonShortO(s: Option[Short]): Json = s.map(Json.fromInt(_)).getOrElse(Json.Null)
    private def json(l: Seq[String]): Json = Json.arr(l.map(Json.fromString).toArray: _*)
    private def json(t: (String, String)): (String, Json) = (t._1, Json.fromString(t._2))
    private def jsonO(t: (String, Option[String])): (String, Json) = (t._1, jsonStringO(t._2))
    private def json(m: Map[String, String]): Json = Json.obj(m.map(json).toArray: _*)

    def migrateDeployedTopology(): DeployedTopologyMigrateResult =
      deployedTopology.topologyWithVersion.map(_.topology) match {
        case Some(topology) => DeployedTopologyMigrateResult.Deploy(migrateTopology(topology))
        case None => DeployedTopologyMigrateResult.Delete(deployedTopology.topologyId.id)
      }

    def migrateRemovedDeployedTopology(): DeployedTopologyMigrateResult.Delete =
      DeployedTopologyMigrateResult.Delete(deployedTopology.topologyId.id)

    private def migrateTopology(topology: TopologyToDeploy): Json = {
      Json.obj(
        Array(
          "id" -> Json.fromString(topology.topologyEntityId.id),
          "namespace" -> Json.fromString(topology.namespace.ns),
          "description" -> jsonStringO(topology.description),
          "developers" -> Json.arr(topology.developers.map(Json.fromString).toArray: _*),
          "developersAccess" -> migrateDevelopersAccess(topology.developersAccess),
          // deployWithAuthorizationCode is ignored because the new system does not have a concept of authorization-codes
          "topics" -> Json.arr(topology.topics.map(migrateTopic).toArray: _*),
          "applications" -> Json.arr(topology.applications.map(migrateApplication).toArray: _*),
          "aliases" -> Json.obj(
            Array(
              "topics" -> Json.arr(topology.aliases.topics.map(migrateTopicAlias).toArray: _*),
              "applications" -> Json.arr(topology.aliases.applications.map(migrateApplicationAlias).toArray: _*),
            ).withoutEmptyOrNull: _*
          ),
          "relationships" -> Json.arr(topology.relationships.flatMap(migrateRelationship).toArray: _*),
          "tags" -> json(topology.tags),
          "labels" -> json(topology.labels),
        ).withoutEmptyOrNull: _*
      )
    }

    private def migrateDevelopersAccess(developersAccess: DevelopersAccess): Json =
      Json.fromString(
        developersAccess match {
          case DevelopersAccess.Full => "Full"
          case DevelopersAccess.TopicReadOnly => "TopicReadOnly"
        }
      )

    private def migrateTopic(topicIdTopic: (LocalTopicId, TopologyToDeploy.Topic)): Json = {
      val (localTopicId, topic) = topicIdTopic
      // TODO migration - do we want to override the topic id for migrated topologies? there are pros and cons, but for now let's not do it
      Json.obj(
        Array(
          "name" -> Json.fromString(topic.name),
          "unManaged" -> Json.fromBoolean(topic.unManaged),
          "partitions" -> Json.fromInt(topic.partitions),
          "replicationFactor" -> jsonShortO(topic.replicationFactor),
          "configDefaults" -> jsonStringO(topic.replicaPlacement.map(_.id)),
          "config" -> json(topic.config),
          "description" -> jsonStringO(topic.description),
          // TODO migration - remove the same/child namespaces of the current topology's namespace, those can consume automatically
          "canBeConsumedBy" -> Json.arr(topic.otherConsumerNamespaces.map(migrateFlexibleName("otherConsumerNamespaces")).toArray: _*),
          "canBeProducedBy" -> Json.arr(topic.otherProducerNamespaces.map(migrateFlexibleName("otherProducerNamespaces")).toArray: _*),
          "tags" -> json(topic.tags),
          "labels" -> json(topic.labels),
        ).filter { case (k, v) => k != "unManaged" || v.asBoolean.exists(v => v) } // unManaged = false is filtered out, that's the default anyway and almost all topics have it
         .withoutEmptyOrNull: _*
      )
    }

    private def migrateFlexibleName(property: String)(flexibleName: FlexibleName): Json = {
      // The original flexibleName is for kafkakewl namespaces.
      // In vnext these will be for topics' and applications' fq ids, which must have the namespace as a prefix.
      flexibleName match {
        // any -> any: it means the same in vnext
        case FlexibleName.Any() => Json.obj(json("type" -> "any"))
        // exact -> namespace: in legacy it meant all topics/applications in exactly the given namespace, in vnext it'll mean the same but in child namespaces too. This should be acceptable.
        // TODO migration - ask users if migrating exact -> namespace is correct. The difference is that we allow child namespaces too.
        case FlexibleName.Exact(value) => Json.obj(json("type" -> "namespace"), json("value" -> value))
        // regex -> regex: it may or may not work in vnext, where we match against fq ids - if needed we can support it, but the migration must check whether the results are the same or the differences are acceptable
        case FlexibleName.Regex(value) => Json.obj(json("type" -> "regex"), json("value" -> value)).andLogMigrationMessage(s"$property / FlexibleName.Regex($flexibleName) isn't supported in vnext")
        // prefix -> namespace: not exactly correct, it won't be supported in vnext (regex can cover it)
        case FlexibleName.Prefix(value) => Json.obj(json("type" -> "namespace"), json("value" -> value)).andLogMigrationMessage(s"$property / FlexibleName.Prefix($flexibleName) isn't supported in vnext")
        // namespace -> namespace: it means the same
        case FlexibleName.Namespace(value) => Json.obj(json("type" -> "namespace"), json("value" -> value))
      }
      // TODO migration - ensure that we ended up with the same behaviour in legacy vs vnext - a special end-point in both that dumps for topics/applications the set of other topics/applications that they can have relationships with
    }

    private def migrateApplication(applicationIdApplication: (LocalApplicationId, TopologyToDeploy.Application)): Json = {
      val (localApplicationId, application) = applicationIdApplication
      Json.obj(
        Array(
          "id" -> Json.fromString(localApplicationId.id),
          "user" -> Json.fromString(application.user),
          jsonO("consumerGroup" -> application.simpleConsumerGroup),
          jsonO("transactionalId" -> application.simpleTransactionalId),
          jsonO("kafkaStreamsAppId" -> application.kafkaStreamsAppId),
          jsonO("connector" -> application.connector),
          jsonO("connectReplicator" -> application.connectReplicator),
          "host" -> jsonStringO(application.host),
          "description" -> jsonStringO(application.description),
          "canConsume" -> Json.arr(application.otherConsumableNamespaces.map(migrateFlexibleName("otherConsumableNamespaces")).toArray: _*),
          "canProduce" -> Json.arr(application.otherProducableNamespaces.map(migrateFlexibleName("otherProducableNamespaces")).toArray: _*),
          "consumerLagWindowSeconds" -> jsonIntO(application.consumerLagWindowSeconds),
          "tags" -> json(application.tags),
          "labels" -> json(application.labels),
        ).withoutEmptyOrNull: _*
      )
    }

    private def migrateTopicAlias(aliasIdNodes: (LocalAliasId, Seq[FlexibleNodeId])): Json = {
      val (localAliasId, nodes) = aliasIdNodes
      val nodesJson = Json.arr(
        nodes.map {
          case FlexibleNodeId.Exact(value, None) =>
            // Need to map the legacy local or fq topic id to vnext's full topic name
            val localTopic = topologyToDeployOption.get.topics.get(LocalTopicId(value))
            val fullyQualifiedTopic = topicDetailsById.get(TopicId(value)).map(_.topic)
            val topic = localTopic.orElse(fullyQualifiedTopic).getOrElse { fail(s"Invalid topic alias Exact($value)") }
            Json.obj(json("type" -> "exact"), json("value", topic.name))
          // TODO migration - it's hard to correctly convert legacy fq topic id regexes to vnext topic name regexes - currently this is being used by one topology where it works out fine - needs testing after conversion
          case FlexibleNodeId.Regex(value) => Json.obj(json("type" -> "regex"), json("value", value)).andLogMigrationMessage(s"topicAliases / FlexibleNodeId.Regex($value) isn't supported in vnext")
          // prefix -> regex: not exactly correct, it won't be supported in vnext (regex can cover it)
          case FlexibleNodeId.Prefix(value) => Json.obj(json("type" -> "regex"), json("value", s"^$value.*")).andLogMigrationMessage(s"topicAliases / FlexibleNodeId.Prefix($value) isn't supported in vnext")
          // namespace -> namespace: it means the same
          case FlexibleNodeId.Namespace(value) => Json.obj(json("type" -> "namespace"), json("value", value))
          // This match isn't exhaustive, but we don't expect to get FlexibleNodeId.Exact(value, Some(_))
          case _ => fail("migrateTopicAlias: non-exhaustive match failed")
        }.toArray: _*
      )
      Json.obj(json("id" -> localAliasId.id), "match" -> nodesJson)
    }

    private def migrateApplicationAlias(aliasIdNodes: (LocalAliasId, Seq[FlexibleNodeId])): Json = {
      val (localAliasId, nodes) = aliasIdNodes
      val nodesJson = Json.arr(
        nodes.map {
          case FlexibleNodeId.Exact(value, None) =>
            // Need to map the legacy local or fq application id to vnext's local or fq application id (the difference is that in vnext the fq application id is namespace + local-id)
            val applicationId = if (topologyToDeployOption.get.applications.contains(LocalApplicationId(value))) {
              // it's a local application id, it'll be the same in vnext
              value
            } else {
              applicationDetailsById.get(ApplicationId(value))
                // it must be a fq application id, in which case the vnext application id will be the containing topology's namespace + local
                .map { tnd => tnd.topology.namespace.appendApplicationId(tnd.LocalApplicationId).id }
                .getOrElse { fail(s"Invalid application alias Exact($value)") }
            }
            Json.obj(json("type" -> "exact"), json("value", applicationId))
          // regex -> regex: not exactly correct, but it's not being used right now
          case FlexibleNodeId.Regex(value) => Json.obj(json("type" -> "regex"), json("value", value)).andLogMigrationMessage(s"applicationAliases / FlexibleNodeId.Regex($value) isn't supported in vnext")
          // prefix -> regex: not exactly correct, it won't be supported in vnext (regex can cover it)
          case FlexibleNodeId.Prefix(value) => Json.obj(json("type" -> "regex"), json("value", s"^$value.*")).andLogMigrationMessage(s"applicationAliases / FlexibleNodeId.Prefix($value) isn't supported in vnext")
          // namespace -> namespace: it means the same
          case FlexibleNodeId.Namespace(value) => Json.obj(json("type" -> "namespace"), json("value", value))
          // This match isn't exhaustive, but we don't expect to get FlexibleNodeId.Exact(value, Some(_))
          case _ => fail("migrateApplicationAlias: non-exhaustive match failed")
        }.toArray: _*
      )
      Json.obj(json("id" -> localAliasId.id), "match" -> nodesJson)
    }

    private def migrateRelationship(nodeRefRelationships: (NodeRef, NodeRelationships)): Option[Json] = {
      val (nodeRef, nodeRelationships) = nodeRefRelationships
      findNodeRefDetails(nodeRef) match {
        case Some(nodeDetails) =>
          nodeDetails match {
            case TopologyNodeDetails.Topic(_, _, topic) => none.andLogMigrationMessage(s"relationship starting with a topic $nodeRef (${topic.name}) isn't supported in vnext")
            case TopologyNodeDetails.TopicAlias(_, _, _) => none.andLogMigrationMessage(s"relationship starting with a topic alias $nodeRef isn't supported in vnext")
            case _: TopologyNodeDetails.Application | _: TopologyNodeDetails.ApplicationAlias  =>
              nodeRelationships
                .filter { case (relationshipType, _) => !relationshipType.isConsumeOrProduce }
                .foreach { case (relationshipType, _) => logMigrationMessage(s"special relationship $relationshipType starting with $nodeRef isn't supported") }
              Json.obj(
                Array(
                  "application" -> Json.fromString(localOrFullyQualifiedNodeId(nodeDetails).id),
                  "consume" -> Json.arr(migrateNodeRelationshipNodes(nodeRef, nodeRelationships.get(RelationshipType.Consume()).toSeq.flatten, RelationshipType.Consume(), consumeNodeRelationshipProperties): _*),
                  "produce" -> Json.arr(migrateNodeRelationshipNodes(nodeRef, nodeRelationships.get(RelationshipType.Produce()).toSeq.flatten, RelationshipType.Produce(), produceNodeRelationshipProperties): _*)
                ).withoutEmptyOrNull: _*
              ).some
          }
        case None =>
          if (nodeRelationships.values.flatten.isEmpty) {
            // Unfortunately kafkakewl has a bug - it allows non-existing nodeRefs in relationships. As long as the relationship's other side is empty,
            // we can safely ignore this.
            none.andLogMigrationMessage(s"invalid but empty relationship for $nodeRef is ignored")
          } else {
            fail(s"Invalid relationship node $nodeRef")
          }
      }
    }

    private def consumeNodeRelationshipProperties(fromNodeRef: NodeRef, toNodeRef: NodeRef, properties: TopologyToDeploy.RelationshipProperties): Seq[(String, Json)] = {
      if (properties.reset.nonEmpty) logMigrationMessage(s"consume relationship $fromNodeRef -> $toNodeRef with non-empty reset: ${properties.reset}")
      Seq(
        properties.monitorConsumerLag.map(l => "monitorConsumerLag" -> Json.fromBoolean(l)),
        ("tags" -> json(properties.tags)).some,
        ("labels" -> json(properties.labels)).some,
      ).flatten
    }

    private def produceNodeRelationshipProperties(fromNodeRef: NodeRef, toNodeRef: NodeRef, properties: TopologyToDeploy.RelationshipProperties): Seq[(String, Json)] = {
      if (properties.reset.nonEmpty) logMigrationMessage(s"produce relationship $fromNodeRef -> $toNodeRef with non-empty reset: ${properties.reset}")
      if (properties.monitorConsumerLag.nonEmpty) logMigrationMessage(s"produce relationship $fromNodeRef -> $toNodeRef with non-empty monitorConsumerLag: ${properties.monitorConsumerLag}")
      Seq(
        ("tags" -> json(properties.tags)).some,
        ("labels" -> json(properties.labels)).some,
      ).flatten
    }

    private def migrateNodeRelationshipNodes(
      fromNodeRef: NodeRef,
      toNodes: Seq[TopologyToDeploy.NodeRelationshipProperties],
      relationshipType: RelationshipType,
      propertiesFunc: (NodeRef, NodeRef, TopologyToDeploy.RelationshipProperties) => Seq[(String, Json)],
    ): Array[Json] = {
      val relType = relationshipType.toString.toLowerCase
      toNodes
        .flatMap { toNode =>
          val toNodeRef = toNode.id
          findNodeRefDetails(toNodeRef)
            .flatMap { nodeDetails =>
              nodeDetails match {
                case TopologyNodeDetails.Application(_, _, _) => none.andLogMigrationMessage(s"$relType relationship $fromNodeRef -> $toNodeRef ending with an application isn't supported in vnext")
                case TopologyNodeDetails.ApplicationAlias(_, _, _) => none.andLogMigrationMessage(s"$relType relationship $fromNodeRef -> $toNodeRef ending with an application alias isn't supported in vnext")
                case _: TopologyNodeDetails.Topic | _: TopologyNodeDetails.TopicAlias =>
                  Json.obj(
                    Array("topic" -> Json.fromString(localOrFullyQualifiedNodeId(nodeDetails).id)) ++ propertiesFunc(fromNodeRef, toNodeRef, toNode.properties).toArray.withoutEmptyOrNull: _*
                  ).some
              }
            }
        }
        .toArray
    }

    private def calculateTopologyById(currentDeployedTopology: Option[DeployedTopology] = none) =
      currentDeployedTopology.map(current => deployedTopologies + (current.id -> current)).getOrElse(deployedTopologies).values
        .flatMap(_.topologyWithVersion.map(_.topology))
        .map(t => (t.topologyEntityId, t))
        .toMap

    private def calculateTopicDetailsById(topologies: Iterable[TopologyToDeploy]) =
      topologies
        .flatMap { topology =>
          topology.topics
            .map { case (id, topic) => (topology.fullyQualifiedTopicId(id), TopologyNodeDetails.Topic(topology, id, topic)) }
        }
        .toMap

    private def calculateApplicationDetailsById(topologies: Iterable[TopologyToDeploy]) =
      topologies
        .flatMap { topology =>
          topology.applications
            .map { case (id, application) => (topology.fullyQualifiedApplicationId(id), TopologyNodeDetails.Application(topology, id, application)) }
        }
        .toMap

    private def getTopologyNodeDetails(nodeId: NodeId): Option[TopologyNodeDetails] =
      topicDetailsByIdWithCurrent.get(nodeId.toTopicId) orElse applicationDetailsByIdWithCurrent.get(nodeId.toApplicationId)

    private def findNodeRefDetails(nodeRef: NodeRef): Option[TopologyNodeDetails] = {
      val topology = topologyToDeployOption.get

      def getLocalAliasDetails(aliasId: LocalAliasId): Option[TopologyNodeDetails] =
        topology.aliases.topics.get(aliasId).map { TopologyNodeDetails.TopicAlias(topology, aliasId, _) } orElse
          topology.aliases.applications.get(aliasId).map { TopologyNodeDetails.ApplicationAlias(topology, aliasId, _) }

      def getFullyQualifiedAliasDetails(aliasId: AliasId): Option[TopologyNodeDetails] =
        topology.aliases.topics
          .find { case (localAliasId, _) => topology.fullyQualifiedAliasId(localAliasId) == aliasId }
          .map { case (localAliasId, tf) => TopologyNodeDetails.TopicAlias(topology, localAliasId, tf) } orElse
          topology.aliases.applications
            .find { case (localAliasId, _) => topology.fullyQualifiedAliasId(localAliasId) == aliasId }
            .map { case (localAliasId, tf) => TopologyNodeDetails.ApplicationAlias(topology, localAliasId, tf) }


      // trying it as a local topic or application id
      getTopologyNodeDetails(topology.fullyQualifiedNodeId(LocalNodeId(nodeRef))) orElse
        // trying it as a fully qualified topic or application id
        getTopologyNodeDetails(NodeId(nodeRef)) orElse
        // trying it as a local topic or application alias id
        getLocalAliasDetails(LocalAliasId(nodeRef)) orElse
        // trying it as a fully-qualified topic or application alias id
        getFullyQualifiedAliasDetails(AliasId(nodeRef))
    }

    private def localOrFullyQualifiedNodeId(topologyNodeDetails: TopologyNodeDetails): NodeRef = {
      val topologyId = topologyNodeDetails.topology.topologyEntityId
      topologyNodeDetails match {
        case topic: TopologyNodeDetails.Topic =>
          // topics always have their full topic name as the id
          NodeRef(topic.topic.name)

        case application: TopologyNodeDetails.Application if topologyId == deployedTopology.topologyId =>
          // applications in the same topology can have their local id (it's the same in vnext)
          NodeRef(application.localId.id)

        case application: TopologyNodeDetails.Application if topologyId != deployedTopology.topologyId =>
          // applications in different topologies need their vnext fq id (it's namespace + local-id in vnext!!!)
          NodeRef(application.FullyQualifiedIdVnext)

        case topicAlias: TopologyNodeDetails.TopicAlias =>
          // aliases cannot be used in different topologies
          if (topologyId != deployedTopology.topologyId) {
            fail(s"topic-alias ${topicAlias.localId} in $topologyId cannot be used this topology")
          } else {
            NodeRef(topicAlias.localId.id)
          }

        case applicationAlias: TopologyNodeDetails.ApplicationAlias =>
          // aliases cannot be used in different topologies
          if (topologyId != deployedTopology.topologyId) {
            fail(s"application-alias ${applicationAlias.localId} in $topologyId cannot be used this topology")
          } else {
            NodeRef(applicationAlias.localId.id)
          }
      }
    }

    private def fail(message: String): Nothing = {
      sys.error(s"failed migrating ${deployedTopology.kafkaClusterId} / ${deployedTopology.topologyId} / v${deployedTopology.deploymentVersion}: $message")
    }

    private def logMigrationMessage(message: String): Unit = {
      withMDC(getCurrentMDC ++ Map("eventId" -> "migration")) {
        logger.warn(s"migrating ${deployedTopology.kafkaClusterId} / ${deployedTopology.topologyId} / v${deployedTopology.deploymentVersion}: $message")
      }
    }

    private implicit class LogExtensions[T](obj: T) {
      def and(sideEffect: => Unit): T = {
        sideEffect
        obj
      }

      def andLogMigrationMessage(message: String): T = obj.and { logMigrationMessage(message) }
    }

    private implicit class KeyedJsonExtensions(obj: Array[(String, Json)]) {
      def withoutEmptyOrNull: Array[(String, Json)] =
        obj.filter { case (_, value) => !value.isNull && value.asArray.forall(_.nonEmpty) && value.asObject.forall(_.nonEmpty) }
    }
  }
}
