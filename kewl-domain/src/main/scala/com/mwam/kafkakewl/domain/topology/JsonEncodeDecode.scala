/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import cats.syntax.either._
import cats.syntax.functor._
import io.circe.{KeyDecoder, KeyEncoder}

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto._
import io.circe.syntax._

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._

  implicit class MapKeyedValuedByStringExtensions(map: Map[String, String]) {
    def toLabelMap: Topology.LabelsExpr = map.map { case (k, v) => (Topology.LabelKeyExpr(k), Topology.LabelValueExpr(v)) }
  }

  def encoderWithoutDefaultTopologyTopicProperties(en: Encoder[Topology.Topic]): Encoder[Topology.Topic] = {
    encoderWithoutDefaultLabelledWithExpr(en)
      .mapJson(
        removeFieldIfNull("replicationFactor") andThen
        removeArrayFieldIfEmpty("otherConsumerNamespaces") andThen
        removeArrayFieldIfEmpty("otherProducerNamespaces") andThen
        removeFieldIfNull("description") andThen
        removeFieldIfNull("replicaPlacement") andThen
        removeFieldIf("unManaged", defaultValue = false)
      )
  }

  def encoderWithoutDefaultTopologyToDeployTopicProperties(en: Encoder[TopologyToDeploy.Topic]): Encoder[TopologyToDeploy.Topic] = {
    encoderWithoutDefaultLabelled(en)
      .mapJson(
        removeFieldIfNull("replicationFactor") andThen
        removeArrayFieldIfEmpty("otherConsumerNamespaces") andThen
        removeArrayFieldIfEmpty("otherProducerNamespaces") andThen
        removeFieldIfNull("description") andThen
        removeFieldIfNull("replicaPlacement") andThen
        removeFieldIf("unManaged", defaultValue = false)
      )
  }

  def encoderWithoutDefaultTopologyApplicationProperties(en: Encoder[Topology.Application]): Encoder[Topology.Application] = {
    encoderWithoutDefaultLabelledWithExpr(en)
      .mapJson(
        removeArrayFieldIfEmpty("otherConsumableNamespaces") andThen
          removeArrayFieldIfEmpty("otherProducableNamespaces") andThen
          removeFieldIfNull("description") andThen
          removeFieldIfNull("host")
      )
  }

  def encoderWithoutDefaultTopologyToDeployApplicationProperties(en: Encoder[TopologyToDeploy.Application]): Encoder[TopologyToDeploy.Application] = {
    encoderWithoutDefaultLabelled(en)
      .mapJson(
        removeArrayFieldIfEmpty("otherConsumableNamespaces") andThen
          removeArrayFieldIfEmpty("otherProducableNamespaces") andThen
          removeFieldIfNull("description") andThen
          removeFieldIfNull("host")
      )
  }

  // FlexibleNodeId
  implicit val flexibleNodeIdEncoder: Encoder[FlexibleNodeId] = Encoder.instance {
    case n: FlexibleNodeId.Exact => n.exact.asJson
    case n: FlexibleNodeId.Regex => n.asJson
    case n: FlexibleNodeId.Prefix => n.asJson
    case n: FlexibleNodeId.Namespace => n.asJson
  }
  implicit val flexibleNodeIdDecoder: Decoder[FlexibleNodeId] =
    List[Decoder[FlexibleNodeId]](
      Decoder[String].map(FlexibleNodeId.Exact.apply(_)).widen,
      Decoder[FlexibleNodeId.Exact].widen,
      Decoder[FlexibleNodeId.Regex].widen,
      Decoder[FlexibleNodeId.Prefix].widen,
      Decoder[FlexibleNodeId.Namespace].widen,
    ).reduceLeft(_ or _)

  // FlexibleTopologyTopicId
  private final case class FlexibleTopologyTopicIdAnyForJson(any: Boolean)
  implicit val flexibleTopologyTopicIdEncoder: Encoder[FlexibleTopologyTopicId] = Encoder.instance {
    case n: FlexibleTopologyTopicId.Any => FlexibleTopologyTopicIdAnyForJson(true).asJson
    case n: FlexibleTopologyTopicId.Exact => n.exact.asJson
    case n: FlexibleTopologyTopicId.Regex => n.asJson
    case n: FlexibleTopologyTopicId.Prefix => n.asJson
  }
  implicit val flexibleTopologyTopicIdDecoder: Decoder[FlexibleTopologyTopicId] =
    List[Decoder[FlexibleTopologyTopicId]](
      // a little hack do decode { "any": true } to FlexibleTopologyTopicId.Any and fail (try the next one) on anything else
      Decoder[FlexibleTopologyTopicIdAnyForJson].flatMap(a => if (a.any) Decoder[FlexibleTopologyTopicId.Any].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[String].map(FlexibleTopologyTopicId.Exact.apply(_)).widen,
      Decoder[FlexibleTopologyTopicId.Exact].widen,
      Decoder[FlexibleTopologyTopicId.Regex].widen,
      Decoder[FlexibleTopologyTopicId.Prefix].widen
    ).reduceLeft(_ or _)

  // ResetMode
  implicit val resetModeEncoder: Encoder[ResetMode] = deriveEnumerationEncoder
  implicit val resetModeDecoder: Decoder[ResetMode] = deriveEnumerationDecoder

  // relationship-type
  implicit val relationshipTypeEncoder: Encoder[RelationshipType] = Encoder[String].contramap(_.toString)
  implicit val relationshipTypeDecoder: Decoder[RelationshipType] = Decoder[String].map(RelationshipType.apply)
  implicit val relationshipTypeKeyEncoder: KeyEncoder[RelationshipType] = (relType: RelationshipType) => relType.toString
  implicit val relationshipTypeKeyDecoder: KeyDecoder[RelationshipType] = (key: String) => Some(RelationshipType(key))

  // id types
  implicit val localNodeIdEncoder: Encoder[LocalNodeId] = Encoder[String].contramap(_.id)
  implicit val localNodeIdDecoder: Decoder[LocalNodeId] = Decoder[String].map(LocalNodeId(_))
  implicit val nodeIdEncoder: Encoder[NodeId] = Encoder[String].contramap(_.id)
  implicit val nodeIdDecoder: Decoder[NodeId] = Decoder[String].map(NodeId(_))
  implicit val localNodeIdKeyEncoder: KeyEncoder[LocalNodeId] = (n: LocalNodeId) => n.id
  implicit val localNodeIdKeyDecoder: KeyDecoder[LocalNodeId] = (n: String) => Some(LocalNodeId(n))
  implicit val nodeIdKeyEncoder: KeyEncoder[NodeId] = (n: NodeId) => n.id
  implicit val nodeIdKeyDecoder: KeyDecoder[NodeId] = (n: String) => Some(NodeId(n))
  implicit val localTopicIdEncoder: Encoder[LocalTopicId] = Encoder[String].contramap(_.id)
  implicit val localTopicIdDecoder: Decoder[LocalTopicId] = Decoder[String].map(LocalTopicId)
  implicit val topicIdEncoder: Encoder[TopicId] = Encoder[String].contramap(_.id)
  implicit val topicIdDecoder: Decoder[TopicId] = Decoder[String].map(TopicId)
  implicit val localTopicIdKeyEncoder: KeyEncoder[LocalTopicId] = (n: LocalTopicId) => n.id
  implicit val localTopicIdKeyDecoder: KeyDecoder[LocalTopicId] = (n: String) => Some(LocalTopicId(n))
  implicit val topicIdKeyEncoder: KeyEncoder[TopicId] = (n: TopicId) => n.id
  implicit val topicIdKeyDecoder: KeyDecoder[TopicId] = (n: String) => Some(TopicId(n))
  implicit val localApplicationIdEncoder: Encoder[LocalApplicationId] = Encoder[String].contramap(_.id)
  implicit val localApplicationIdDecoder: Decoder[LocalApplicationId] = Decoder[String].map(LocalApplicationId)
  implicit val applicationIdEncoder: Encoder[ApplicationId] = Encoder[String].contramap(_.id)
  implicit val applicationIdDecoder: Decoder[ApplicationId] = Decoder[String].map(ApplicationId)
  implicit val localApplicationIdKeyEncoder: KeyEncoder[LocalApplicationId] = (n: LocalApplicationId) => n.id
  implicit val localApplicationIdKeyDecoder: KeyDecoder[LocalApplicationId] = (n: String) => Some(LocalApplicationId(n))
  implicit val applicationIdKeyEncoder: KeyEncoder[ApplicationId] = (n: ApplicationId) => n.id
  implicit val applicationIdKeyDecoder: KeyDecoder[ApplicationId] = (n: String) => Some(ApplicationId(n))
  implicit val localAliasIdEncoder: Encoder[LocalAliasId] = Encoder[String].contramap(_.id)
  implicit val localAliasIdDecoder: Decoder[LocalAliasId] = Decoder[String].map(LocalAliasId.apply)
  implicit val aliasIdEncoder: Encoder[AliasId] = Encoder[String].contramap(_.id)
  implicit val aliasIdDecoder: Decoder[AliasId] = Decoder[String].map(AliasId.apply)
  implicit val localAliasKeyEncoder: KeyEncoder[LocalAliasId] = (n: LocalAliasId) => n.id
  implicit val localAliasKeyDecoder: KeyDecoder[LocalAliasId] = (n: String) => Some(LocalAliasId(n))
  implicit val aliasKeyEncoder: KeyEncoder[AliasId] = (n: AliasId) => n.id
  implicit val aliasKeyDecoder: KeyDecoder[AliasId] = (n: String) => Some(AliasId(n))
  implicit val topologyIdEncoder: Encoder[TopologyId] = Encoder[String].contramap(_.id)
  implicit val topologyIdDecoder: Decoder[TopologyId] = Decoder[String].map(TopologyId)
  implicit val topologyIdKeyEncoder: KeyEncoder[TopologyId] = (n: TopologyId) => n.id
  implicit val topologyIdKeyDecoder: KeyDecoder[TopologyId] = (n: String) => Some(TopologyId(n))

  // developers-access
  implicit val topologyDevelopersAccessEncoder: Encoder[DevelopersAccess] = deriveEnumerationEncoder
  implicit val topologyDevelopersAccessDecoder: Decoder[DevelopersAccess] = deriveEnumerationDecoder

  // nodes-id, key
  implicit val topologyNodeRefEncoder: Encoder[NodeRef] = Encoder[String].contramap(_.id)
  implicit val topologyNodeRefDecoder: Decoder[NodeRef] = Decoder[String].map(NodeRef)
  implicit val topologyNodeRefKeyEncoder: KeyEncoder[NodeRef] = (nodeRef: NodeRef) => nodeRef.id
  implicit val topologyNodeRefKeyDecoder: KeyDecoder[NodeRef] = (key: String) => Some(NodeRef(key))

  /**
    * Topology and related types
    */

  // expressions
  implicit val developersAccessExprEncoder: Encoder[Topology.DevelopersAccessExpr] = encoderString(_.expr)
  implicit val developersAccessExprDecoder: Decoder[Topology.DevelopersAccessExpr] = decoderString(Topology.DevelopersAccessExpr)
  implicit val deployWithAuthorizationCodeExprEncoder: Encoder[Topology.DeployWithAuthorizationCodeExpr] = encoderString(_.expr)
  implicit val deployWithAuthorizationCodeExprDecoder: Decoder[Topology.DeployWithAuthorizationCodeExpr] = decoderString(Topology.DeployWithAuthorizationCodeExpr)
  implicit val tagExprEncoder: Encoder[Topology.TagExpr] = encoderString(_.expr)
  implicit val tagExprDecoder: Decoder[Topology.TagExpr] = decoderString(Topology.TagExpr)
  implicit val labelKeyExprEncoder: Encoder[Topology.LabelKeyExpr] = encoderString(_.expr)
  implicit val labelKeyExprDecoder: Decoder[Topology.LabelKeyExpr] = decoderString(Topology.LabelKeyExpr)
  implicit val labelValueExprEncoder: Encoder[Topology.LabelValueExpr] = encoderString(_.expr)
  implicit val labelValueExprDecoder: Decoder[Topology.LabelValueExpr] = decoderString(Topology.LabelValueExpr)
  implicit val labelsExprEncoder: Encoder[Topology.LabelsExpr] = Encoder[Map[String, String]].contramap(_.toMapByString)
  implicit val labelsExprDecoder: Decoder[Topology.LabelsExpr] = Decoder[Map[String, String]].map(_.toLabelMap)
  implicit val partitionsExprEncoder: Encoder[Topology.PartitionsExpr] = encoderIntOrString(_.expr)
  implicit val partitionsExprDecoder: Decoder[Topology.PartitionsExpr] = decoderIntOrString(Topology.PartitionsExpr)
  implicit val replicationFactorExprEncoder: Encoder[Topology.ReplicationFactorExpr] = encoderShortOrString(_.expr)
  implicit val replicationFactorExprDecoder: Decoder[Topology.ReplicationFactorExpr] = decoderShortOrString(Topology.ReplicationFactorExpr)
  implicit val topicConfigExprEncoder: Encoder[Topology.TopicConfigExpr] = encoderString(_.expr)
  implicit val topicConfigExprDecoder: Decoder[Topology.TopicConfigExpr] = decoderString(Topology.TopicConfigExpr)
  implicit val replicaPlacementExprEncoder: Encoder[Topology.ReplicaPlacementExpr] = encoderString(_.expr)
  implicit val replicaPlacementExprDecoder: Decoder[Topology.ReplicaPlacementExpr] = decoderString(Topology.ReplicaPlacementExpr)
  implicit val userExprEncoder: Encoder[Topology.UserExpr] = encoderString(_.expr)
  implicit val userExprDecoder: Decoder[Topology.UserExpr] = decoderString(Topology.UserExpr)
  implicit val consumerLagWindowSecondsExprEncoder: Encoder[Topology.ConsumerLagWindowSecondsExpr] = encoderIntOrString(_.expr)
  implicit val consumerLagWindowSecondsExprDecoder: Decoder[Topology.ConsumerLagWindowSecondsExpr] = decoderIntOrString(Topology.ConsumerLagWindowSecondsExpr)

  // topics, applications
  implicit val topologyTopicEncoder: Encoder[Topology.Topic] = encoderWithoutDefaultTopologyTopicProperties(deriveConfiguredEncoder)
  implicit val topologyTopicDecoder: Decoder[Topology.Topic] = deriveConfiguredDecoder
  implicit val topologyApplicationEncoder: Encoder[Topology.Application] = encoderWithoutDefaultTopologyApplicationProperties(deriveConfiguredEncoder)
  implicit val topologyApplicationDecoder: Decoder[Topology.Application] = deriveConfiguredDecoder

  implicit val topologyRelationshipPropertiesEncoder: Encoder[Topology.RelationshipProperties] = encoderWithoutDefaultLabelledWithExpr(
    deriveConfiguredEncoder[Topology.RelationshipProperties])
      .mapJson(removeFieldIfNull("reset") andThen removeFieldIfNull("monitorConsumerLag"))
  implicit val topologyRelationshipPropertiesDecoder: Decoder[Topology.RelationshipProperties] = deriveConfiguredDecoder

  implicit val topologyNodeRelationshipPropertiesEncoder: Encoder[Topology.NodeRelationshipProperties] = {
    case r: Topology.NodeRelationshipProperties => Map(r.id -> r.properties).asJson
  }
  implicit val topologyNodeRelationshipPropertiesDecoder: Decoder[Topology.NodeRelationshipProperties] = List[Decoder[Topology.NodeRelationshipProperties]](
    Decoder[String].map(s => Topology.NodeRelationshipProperties(NodeRef(s))).widen,
    Decoder[Map[String, Topology.RelationshipProperties]].emap {
      case m if m.isEmpty => "relationship object must have exactly one key but it was empty".asLeft
      case m if m.size == 1 => m.head._2.withNodeRef(NodeRef(m.head._1)).asRight
      case m if m.size > 1 => s"relationship object must have exactly one key but had ${m.size}: ${m.keys.mkString("[", ", ", "]")}".asLeft
    }.widen
  ).reduce(_ or _)

  // Topology.Topics
  implicit val topologyTopicsEncoder: Encoder[Map[LocalTopicId, Topology.Topic]] = Encoder[Map[String, Topology.Topic]].contramap(_.toMapByString )
  implicit val topologyTopicsDecoder: Decoder[Map[LocalTopicId, Topology.Topic]] = Decoder[Map[String, Topology.Topic]].map(_.toMapByLocalTopicId)

  // Topology.Application.Type
  private final case class TopologyApplicationTypeSimpleForJson(consumerGroup: Option[String] = None, transactionalId: Option[String] = None)
  private final case class TopologyApplicationTypeKafkaStreamsWithAppIdForJson(kafkaStreamsAppId: String)
  private final case class TopologyApplicationTypeConnectorForJson(connector: String)
  private final case class TopologyApplicationTypeConnectReplicatorForJson(connectReplicator: String)
  implicit val topologyApplicationTypeEncoder: Encoder[Topology.Application.Type] = Encoder.instance {
    case t: Topology.Application.Type.Simple => TopologyApplicationTypeSimpleForJson(t.consumerGroup, t.transactionalId).asJson
    case t: Topology.Application.Type.KafkaStreams => TopologyApplicationTypeKafkaStreamsWithAppIdForJson(t.kafkaStreamsAppId).asJson
    case t: Topology.Application.Type.Connector => TopologyApplicationTypeConnectorForJson(t.connector).asJson
    case t: Topology.Application.Type.ConnectReplicator => TopologyApplicationTypeConnectReplicatorForJson(t.connectReplicator).asJson
  }
  implicit val topologyApplicationTypeDecoder: Decoder[Topology.Application.Type] =
    List[Decoder[Topology.Application.Type]](
      Decoder[TopologyApplicationTypeKafkaStreamsWithAppIdForJson].map(_.kafkaStreamsAppId).map(Topology.Application.Type.KafkaStreams).widen,
      Decoder[TopologyApplicationTypeConnectorForJson].map(_.connector).map(Topology.Application.Type.Connector).widen,
      Decoder[TopologyApplicationTypeConnectReplicatorForJson].map(_.connectReplicator).map(Topology.Application.Type.ConnectReplicator).widen,
      // this must come last, because any object (even empty) can de decoded into this
      Decoder[TopologyApplicationTypeSimpleForJson].map(j => Topology.Application.Type.Simple(j.consumerGroup, j.transactionalId)).widen
    ).reduceLeft(_ or _)

  // resolved topology and related types
  implicit val topologyNodeIdNodeIdRelationshipEncoder: Encoder[Topology.NodeIdNodeIdRelationship] = deriveConfiguredEncoder
  implicit val topologyNodeIdNodeIdRelationshipDecoder: Decoder[Topology.NodeIdNodeIdRelationship] = deriveConfiguredDecoder
  implicit val resolvedTopologyEncoder: Encoder[Topology.ResolvedTopology] = encoderWithoutDefaultLabelledWithExpr(deriveConfiguredEncoder)
  implicit val resolvedTopologyDecoder: Decoder[Topology.ResolvedTopology] = deriveConfiguredDecoder

  // entities
  implicit val topologyEncoder: Encoder[Topology] = encoderWithoutDefaultLabelledWithExpr(deriveConfiguredEncoder)
  implicit val topologyDecoder: Decoder[Topology] = deriveConfiguredDecoder

  // entity state changes
  implicit val topologyEntityStateChangeEncoder: Encoder[TopologyStateChange.StateChange] = deriveConfiguredEncoder
  implicit val topologyEntityStateChangeDecoder: Decoder[TopologyStateChange.StateChange] = deriveConfiguredDecoder

  /**
    * TopologyToDeploy and related types
    */

  implicit val topologyToDeployTopicEncoder: Encoder[TopologyToDeploy.Topic] = encoderWithoutDefaultTopologyToDeployTopicProperties(deriveConfiguredEncoder)
  implicit val topologyToDeployTopicDecoder: Decoder[TopologyToDeploy.Topic] = deriveConfiguredDecoder
  implicit val topologyToDeployApplicationEncoder: Encoder[TopologyToDeploy.Application] = encoderWithoutDefaultTopologyToDeployApplicationProperties(deriveConfiguredEncoder)
  implicit val topologyToDeployApplicationDecoder: Decoder[TopologyToDeploy.Application] = deriveConfiguredDecoder

  implicit val topologyToDeployRelationshipPropertiesEncoder: Encoder[TopologyToDeploy.RelationshipProperties] = encoderWithoutDefaultLabelled(
    deriveConfiguredEncoder[TopologyToDeploy.RelationshipProperties]).mapJson(removeFieldIfNull("reset") andThen removeFieldIfNull("monitorConsumerLag"))
  implicit val topologyToDeployRelationshipPropertiesDecoder: Decoder[TopologyToDeploy.RelationshipProperties] = deriveConfiguredDecoder

  implicit val topologyToDeployNodeRelationshipPropertiesEncoder: Encoder[TopologyToDeploy.NodeRelationshipProperties] = {
    case r: TopologyToDeploy.NodeRelationshipProperties => Map(r.id -> r.properties).asJson
  }
  implicit val topologyToDeployNodeRelationshipPropertiesDecoder: Decoder[TopologyToDeploy.NodeRelationshipProperties] = List[Decoder[TopologyToDeploy.NodeRelationshipProperties]](
    Decoder[String].map(s => TopologyToDeploy.NodeRelationshipProperties(NodeRef(s))).widen,
    Decoder[Map[String, TopologyToDeploy.RelationshipProperties]].map(m => m.head._2.withNodeRef(NodeRef(m.head._1))).widen
  ).reduce(_ or _)

  // TopologyToDeploy.Topics
  implicit val topologyToDeployTopicsEncoder: Encoder[Map[LocalTopicId, TopologyToDeploy.Topic]] = Encoder[Map[String, TopologyToDeploy.Topic]].contramap(_.toMapByString )
  implicit val topologyToDeployTopicsDecoder: Decoder[Map[LocalTopicId, TopologyToDeploy.Topic]] = Decoder[Map[String, TopologyToDeploy.Topic]].map(_.toMapByLocalTopicId)

  // TopologyToDeploy.Applications
  implicit val topologyToDeployApplicationsEncoder: Encoder[Map[LocalApplicationId, TopologyToDeploy.Application]] = Encoder[Map[String, TopologyToDeploy.Application]].contramap(_.toMapByString)
  implicit val topologyToDeployApplicationsDecoder: Decoder[Map[LocalApplicationId, TopologyToDeploy.Application]] = Decoder[Map[String, TopologyToDeploy.Application]].map(_.toMapByLocalApplicationId)

  // TopologyToDeploy.Application.Type
  private final case class TopologyToDeployApplicationTypeSimpleForJson(consumerGroup: Option[String] = None, transactionalId: Option[String] = None)
  private final case class TopologyToDeployApplicationTypeKafkaStreamsWithAppIdForJson(kafkaStreamsAppId: String)
  private final case class TopologyToDeployApplicationTypeConnectorForJson(connector: String)
  private final case class TopologyToDeployApplicationTypeConnectReplicatorForJson(connectReplicator: String)
  implicit val topologyToDeployApplicationTypeEncoder: Encoder[TopologyToDeploy.Application.Type] = Encoder.instance {
    case t: TopologyToDeploy.Application.Type.Simple => TopologyToDeployApplicationTypeSimpleForJson(t.consumerGroup, t.transactionalId).asJson
    case t: TopologyToDeploy.Application.Type.KafkaStreams => TopologyToDeployApplicationTypeKafkaStreamsWithAppIdForJson(t.kafkaStreamsAppId).asJson
    case t: TopologyToDeploy.Application.Type.Connector => TopologyToDeployApplicationTypeConnectorForJson(t.connector).asJson
    case t: TopologyToDeploy.Application.Type.ConnectReplicator => TopologyToDeployApplicationTypeConnectReplicatorForJson(t.connectReplicator).asJson
  }
  implicit val topologyToDeployApplicationTypeDecoder: Decoder[TopologyToDeploy.Application.Type] =
    List[Decoder[TopologyToDeploy.Application.Type]](
      Decoder[TopologyToDeployApplicationTypeKafkaStreamsWithAppIdForJson].map(_.kafkaStreamsAppId).map(TopologyToDeploy.Application.Type.KafkaStreams).widen,
      Decoder[TopologyToDeployApplicationTypeConnectorForJson].map(_.connector).map(TopologyToDeploy.Application.Type.Connector).widen,
      Decoder[TopologyToDeployApplicationTypeConnectReplicatorForJson].map(_.connectReplicator).map(TopologyToDeploy.Application.Type.ConnectReplicator).widen,
      // this must come last, because any object (even empty) can de decoded into this
      Decoder[TopologyToDeployApplicationTypeSimpleForJson].map(j => TopologyToDeploy.Application.Type.Simple(j.consumerGroup, j.transactionalId)).widen
    ).reduceLeft(_ or _)

  // resolved topology and related types
  implicit val topologyToDeployNodeIdNodeIdRelationshipEncoder: Encoder[TopologyToDeploy.NodeIdNodeIdRelationship] = deriveConfiguredEncoder
  implicit val topologyToDeployNodeIdNodeIdRelationshipDecoder: Decoder[TopologyToDeploy.NodeIdNodeIdRelationship] = deriveConfiguredDecoder
  implicit val resolvedTopologyToDeployEncoder: Encoder[TopologyToDeploy.ResolvedTopology] = encoderWithoutDefaultLabelled(deriveConfiguredEncoder)
  implicit val resolvedTopologyToDeployDecoder: Decoder[TopologyToDeploy.ResolvedTopology] = deriveConfiguredDecoder

  // entities
  implicit val topologyToDeployEncoder: Encoder[TopologyToDeploy] = encoderWithoutDefaultLabelled(deriveConfiguredEncoder)
  implicit val topologyToDeployDecoder: Decoder[TopologyToDeploy] = deriveConfiguredDecoder
}
