/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import scala.reflect.runtime.universe._
import cats.syntax.functor._
import com.mwam.kafkakewl.domain.deploy._
import com.mwam.kafkakewl.domain.kafkacluster._
import com.mwam.kafkakewl.domain.permission._
import com.mwam.kafkakewl.domain.topology._
import io.circe._
import io.circe.generic.extras.auto._
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto._
import io.circe.syntax._
import com.mwam.kafkakewl.utils._

import scala.collection.immutable.ListMap

object JsonEncodeDecodeBase {
  implicit val customConfig: Configuration = Configuration.default.withDefaults

  /**
    * utility functions, implicits
    */

  // utilities
  implicit class MapKeyedByLocalTopicIdExtensions[V](map: Map[LocalTopicId, V]) {
    def toMapByString: Map[String, V] = map.map { case (k, v) => (k.id, v) }
  }
  implicit class MapKeyedByTopicIdExtensions[V](map: Map[TopicId, V]) {
    def toMapByString: Map[String, V] = map.map { case (k, v) => (k.id, v) }
  }
  implicit class MapKeyedByLocalApplicationIdExtensions[V](map: Map[LocalApplicationId, V]) {
    def toMapByString: Map[String, V] = map.map { case (k, v) => (k.id, v) }
  }
  implicit class MapKeyedByApplicationIdExtensions[V](map: Map[ApplicationId, V]) {
    def toMapByString: Map[String, V] = map.map { case (k, v) => (k.id, v) }
  }
  implicit class LabelsExprMapExtensions(map: Topology.LabelsExpr) {
    def toMapByString: Map[String, String] = map.map { case (k, v) => (k.expr, v.expr) }
  }
  implicit class MapKeyedByDeploymentEnvironmentIdExtensions[V](map: Map[DeploymentEnvironmentId, V]) {
    def toMapByString: Map[String, V] = map.map { case (k, v) => (k.id, v) }
  }
  implicit class DeploymentEnvironmentIdValuesExtensions[V](seq: Seq[(DeploymentEnvironmentId, V)]) {
    def toMapByString: ListMap[String, V] = ListMap.empty[String, V] ++ seq.map { case (k, v) => (k.id, v) }
  }
  implicit class MapKeyedByStringExtensions[V](map: Map[String, V]) {
    def toMapByLocalTopicId: Map[LocalTopicId, V] = map.map { case (k, v) => (LocalTopicId(k), v) }
    def toMapByLocalApplicationId: Map[LocalApplicationId, V] = map.map { case (k, v) => (LocalApplicationId(k), v) }
    def toMapByTopicId: Map[TopicId, V] = map.map { case (k, v) => (TopicId(k), v) }
    def toMapByApplicationId: Map[ApplicationId, V] = map.map { case (k, v) => (ApplicationId(k), v) }
    def toMapByDeploymentEnvironmentId: Map[DeploymentEnvironmentId, V] = map.map { case (k, v) => (DeploymentEnvironmentId(k), v) }
  }

  def encoderString[T](f: T => String): Encoder[T] = Encoder.instance[T] { o => f(o).asJson }
  def decoderString[T](f: String => T): Decoder[T] = Decoder[String].map(f)
  def encoderIntOrString[T](f: T => String): Encoder[T] = Encoder.instance[T] { o =>
    val s = f(o)
    s.toIntOrNone.map(_.asJson).getOrElse(s.asJson)
  }
  def decoderIntOrString[T](f: String => T): Decoder[T] =
    List[Decoder[T]](
      Decoder[Int].map(i => f(i.toString)),
      Decoder[String].map(f)
    ).reduceLeft(_ or _)
  def encoderShortOrString[T](f: T => String): Encoder[T] = Encoder.instance[T] { o =>
    val s = f(o)
    s.toShortOrNone.map(_.asJson).getOrElse(s.asJson)
  }
  def decoderShortOrString[T](f: String => T): Decoder[T] =
    List[Decoder[T]](
      Decoder[Short].map(i => f(i.toString)),
      Decoder[String].map(f)
    ).reduceLeft(_ or _)

  def removeArrayFieldIfEmpty(fieldName: String): Json => Json = { json =>
    (for {
      jsonObj <- json.asObject
      field <- jsonObj.toMap.get(fieldName)
      fieldArray <- field.asArray
      if fieldArray.isEmpty
    } yield {
      jsonObj.filterKeys(_ != fieldName).asJson
    }).getOrElse(json)
  }

  def removeObjectFieldIfEmpty(fieldName: String): Json => Json = { json =>
    (for {
      jsonObj <- json.asObject
      field <- jsonObj.toMap.get(fieldName)
      fieldObj <- field.asObject
      if fieldObj.isEmpty
    } yield {
      jsonObj.filterKeys(_ != fieldName).asJson
    }).getOrElse(json)
  }

  def removeFieldIfNull(fieldName: String): Json => Json = { json =>
    (for {
      jsonObj <- json.asObject
      field <- jsonObj.toMap.get(fieldName)
      if field.isNull
    } yield {
      jsonObj.filterKeys(_ != fieldName).asJson
    }).getOrElse(json)
  }

  def removeFieldIf(fieldName: String, defaultValue: Boolean): Json => Json = { json =>
    (for {
      jsonObj <- json.asObject
      field <- jsonObj.toMap.get(fieldName)
      booleanField <- field.asBoolean
      if booleanField.booleanValue() == defaultValue
    } yield {
      jsonObj.filterKeys(_ != fieldName).asJson
    }).getOrElse(json)
  }

  def encoderWithoutDefaultLabelledWithExpr[T <: Topology.LabelledWithExpr](en: Encoder[T]): Encoder[T] = {
    en.mapJson(removeArrayFieldIfEmpty("tags") andThen removeObjectFieldIfEmpty("labels"))
  }

  def encoderWithoutDefaultLabelled[T <: Labelled](en: Encoder[T]): Encoder[T] = {
    en.mapJson(removeArrayFieldIfEmpty("tags") andThen removeObjectFieldIfEmpty("labels"))
  }

  /**
    * various shared types
    */

  // FlexibleName
  private final case class FlexibleNameAnyForJson(any: Boolean)
  implicit val flexibleNameEncoder: Encoder[FlexibleName] = Encoder.instance {
    case n: FlexibleName.Any => FlexibleNameAnyForJson(true).asJson
    // an exact flexible-name gets encoded as a simple string
    case n: FlexibleName.Exact => n.exact.asJson
    case n: FlexibleName.Regex => n.asJson
    case n: FlexibleName.Prefix => n.asJson
    case n: FlexibleName.Namespace => n.asJson
  }
  implicit val flexibleNameDecoder: Decoder[FlexibleName] =
    List[Decoder[FlexibleName]](
      // a little hack do decode { "any": true } to FlexibleName.Any and fail (try the next one) on anything else
      Decoder[FlexibleNameAnyForJson].flatMap(a => if (a.any) Decoder[FlexibleName.Any].widen else Decoder.failedWithMessage("does not matter")),
      // a simple string gets decoded as an exact flexible-name
      Decoder[String].map(FlexibleName.Exact).widen,
      Decoder[FlexibleName.Exact].widen,
      Decoder[FlexibleName.Regex].widen,
      Decoder[FlexibleName.Prefix].widen,
      Decoder[FlexibleName.Namespace].widen
    ).reduceLeft(_ or _)

  // deployment environment variables
  implicit val deploymentVariableValueEncoder: Encoder[DeploymentEnvironment.Variable.Value] = Encoder.instance {
    case DeploymentEnvironment.Variable.Value(Seq()) => Json.Null
    case DeploymentEnvironment.Variable.Value(Seq(single)) => single.asJson
    case DeploymentEnvironment.Variable.Value(values) => values.asJson
  }
  implicit val deploymentVariableValueDecoder: Decoder[DeploymentEnvironment.Variable.Value] =
    List[Decoder[DeploymentEnvironment.Variable.Value]](
      Decoder[Option[String]].map(s => DeploymentEnvironment.Variable.Value(Seq(s).flatten)),
      Decoder[Seq[String]].map(s => DeploymentEnvironment.Variable.Value(s)),
    ).reduceLeft(_ or _)

  // various shared types
  implicit val deploymentEnvironmentIdEncoder: Encoder[DeploymentEnvironmentId] = Encoder[String].contramap(_.id)
  implicit val deploymentEnvironmentIdDecoder: Decoder[DeploymentEnvironmentId] = Decoder[String].map(DeploymentEnvironmentId)

  // deployment environments variables
  implicit val deploymentEnvironmentVariablesEncoder: Encoder[DeploymentEnvironments.Variables] = Encoder[Map[String, DeploymentEnvironment.Variables]].contramap(_.toMapByString)
  implicit val deploymentEnvironmentVariablesDecoder: Decoder[DeploymentEnvironments.Variables] = Decoder[Map[String, DeploymentEnvironment.Variables]].map(_.toMapByDeploymentEnvironmentId)

  // ordered deployment environments variables
  implicit val deploymentEnvironmentOrderedVariablesEncoder: Encoder[DeploymentEnvironments.OrderedVariables] = Encoder[ListMap[String, DeploymentEnvironment.Variables]].contramap(_.toMapByString)
  implicit val deploymentEnvironmentOrderedVariablesDecoder: Decoder[DeploymentEnvironments.OrderedVariables] =
    List[Decoder[DeploymentEnvironments.OrderedVariables]](
      // backwards compatibility: a list of strings also gets deserialized correctly
      Decoder[Seq[String]].map(es => DeploymentEnvironments.OrderedVariables(es.map(id => (DeploymentEnvironmentId(id), DeploymentEnvironment.Variables.empty)): _*)),
      // but from now on, this is really a map that keeps the order of the keys
      Decoder[ListMap[String, DeploymentEnvironment.Variables]].map(_.toSeq.map { case (k, v) => (DeploymentEnvironmentId(k), v) })
    ).reduce(_ or _)

  // ids
  implicit val deploymentIdEntityEncoder: Encoder[DeploymentEntityId] = Encoder[String].contramap(_.id)
  implicit val deploymentIdEntityDecoder: Decoder[DeploymentEntityId] = Decoder[String].map(DeploymentEntityId)
  implicit val deployedTopologyEntityIdEncoder: Encoder[DeployedTopologyEntityId] = Encoder[String].contramap(_.id)
  implicit val deployedTopologyEntityIdDecoder: Decoder[DeployedTopologyEntityId] = Decoder[String].map(DeployedTopologyEntityId)
  implicit val kafkaClusterEntityIdEncoder: Encoder[KafkaClusterEntityId] = Encoder[String].contramap(_.id)
  implicit val kafkaClusterEntityIdDecoder: Decoder[KafkaClusterEntityId] = Decoder[String].map(KafkaClusterEntityId)

  implicit val namespaceEncoder: Encoder[Namespace] = Encoder[String].contramap(_.ns)
  implicit val namespaceDecoder: Decoder[Namespace] = Decoder[String].map(Namespace.apply)
  implicit val namespaceKeyEncoder: KeyEncoder[Namespace] = (n: Namespace) => n.ns
  implicit val namespaceKeyDecoder: KeyDecoder[Namespace] = (n: String) => Some(Namespace(n))
  implicit val topologyEntityIdEncoder: Encoder[TopologyEntityId] = Encoder[String].contramap(_.id)
  implicit val topologyEntityIdDecoder: Decoder[TopologyEntityId] = Decoder[String].map(TopologyEntityId)
  implicit val topologyEntityIdKeyEncoder: KeyEncoder[TopologyEntityId] = (n: TopologyEntityId) => n.id
  implicit val topologyEntityIdKeyDecoder: KeyDecoder[TopologyEntityId] = (n: String) => Some(TopologyEntityId(n))

  implicit val replicaPlacementIdEncoder: Encoder[ReplicaPlacementId] = Encoder[String].contramap(_.id)
  implicit val replicaPlacementIdDecoder: Decoder[ReplicaPlacementId] = Decoder[String].map(ReplicaPlacementId)
  implicit val replicaPlacementIdKeyEncoder: KeyEncoder[ReplicaPlacementId] = (n: ReplicaPlacementId) => n.id
  implicit val replicaPlacementIdKeyDecoder: KeyDecoder[ReplicaPlacementId] = (n: String) => Some(ReplicaPlacementId(n))
}

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._
  import topology.JsonEncodeDecode._
  import kafka.JsonEncodeDecode._
  import kafkacluster.JsonEncodeDecode._
  import deploy.JsonEncodeDecode._
  import permission.JsonEncodeDecode._
  import metrics.JsonEncodeDecode._

  // commands
  implicit val commandEncoder: Encoder[Command] = deriveConfiguredEncoder
  implicit val commandDecoder: Decoder[Command] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandEncoder: Encoder[KafkaClusterCommand] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandDecoder: Decoder[KafkaClusterCommand] = deriveConfiguredDecoder

  // command responses
  implicit val resolvedTopologiesEncoder: Encoder[CommandResponse.ResolvedTopologies] = deriveConfiguredEncoder
  implicit val resolvedTopologiesDecoder: Decoder[CommandResponse.ResolvedTopologies] = deriveConfiguredDecoder
  implicit val resolvedDeployedTopologiesEncoder: Encoder[CommandResponse.ResolvedDeployedTopologies] = deriveConfiguredEncoder
  implicit val resolvedDeployedTopologiesDecoder: Decoder[CommandResponse.ResolvedDeployedTopologies] = deriveConfiguredDecoder

  // entity states (not sure why I need this separately, but otherwise the kewlStateAdaptor stuff doeesn't compile)¬
  implicit val livePermissionEncoder: Encoder[EntityState.Live[Permission]] = deriveConfiguredEncoder
  implicit val livePermissionDecoder: Decoder[EntityState.Live[Permission]] = deriveConfiguredDecoder
  implicit val liveKafkaClusterEncoder: Encoder[EntityState.Live[KafkaCluster]] = deriveConfiguredEncoder
  implicit val liveKafkaClusterDecoder: Decoder[EntityState.Live[KafkaCluster]] = deriveConfiguredDecoder
  implicit val liveTopologyEncoder: Encoder[EntityState.Live[Topology]] = deriveConfiguredEncoder
  implicit val liveTopologyDecoder: Decoder[EntityState.Live[Topology]] = deriveConfiguredDecoder
  implicit val liveDeploymentEncoder: Encoder[EntityState.Live[Deployment]] = deriveConfiguredEncoder
  implicit val liveDeploymentDecoder: Decoder[EntityState.Live[Deployment]] = deriveConfiguredDecoder
  implicit val liveDeployedTopologyEncoder: Encoder[EntityState.Live[DeployedTopology]] = deriveConfiguredEncoder
  implicit val liveDeployedTopologyDecoder: Decoder[EntityState.Live[DeployedTopology]] = deriveConfiguredDecoder

  // types for the command responses
  implicit val commandResponseDeployedApplicationConsumerGroupStatusEncoder: Encoder[CommandResponse.DeployedApplicationStatus.ConsumerGroup] = deriveConfiguredEncoder
  implicit val commandResponseDeployedApplicationConsumerGroupStatusDecoder: Decoder[CommandResponse.DeployedApplicationStatus.ConsumerGroup] = deriveConfiguredDecoder

  // these make json encoding/decoding easier for the different state/state list response types
  // TODO: this is the only place that the compiler doesn't know about when adding a new entity type. It'll fail at runtime if you don't keep this up-to-date.
  private sealed trait CommandResponseForJson
  private final case class TopologyForJson(topology: EntityState.Live[Topology]) extends CommandResponseForJson
  private final case class TopologyCompactForJson(topology: EntityState.Live[TopologyCompact]) extends CommandResponseForJson
  private final case class TopologiesForJson(topologies: Seq[EntityState.Live[Topology]]) extends CommandResponseForJson
  private final case class TopologiesCompactForJson(topologies: Seq[EntityState.Live[TopologyCompact]]) extends CommandResponseForJson
  private final case class KafkaClusterForJson(kafkaCluster: EntityState.Live[KafkaCluster]) extends CommandResponseForJson
  private final case class KafkaClustersForJson(kafkaClusters: Seq[EntityState.Live[KafkaCluster]]) extends CommandResponseForJson
  private final case class PermissionForJson(permission: EntityState.Live[Permission]) extends CommandResponseForJson
  private final case class PermissionsForJson(permissions: Seq[EntityState.Live[Permission]]) extends CommandResponseForJson
  private final case class DeploymentForJson(deployment: EntityState.Live[Deployment]) extends CommandResponseForJson
  private final case class DeploymentsForJson(deployments: Seq[EntityState.Live[Deployment]]) extends CommandResponseForJson
  private final case class DeployedTopologyForJson(deployedTopology: EntityState.Live[DeployedTopology]) extends CommandResponseForJson
  private final case class DeployedTopologyCompactForJson(deployedTopology: EntityState.Live[DeployedTopologyCompact]) extends CommandResponseForJson
  private final case class DeployedTopologiesForJson(deployedTopologies: Seq[EntityState.Live[DeployedTopology]]) extends CommandResponseForJson
  private final case class DeployedTopologiesCompactForJson(deployedTopologies: Seq[EntityState.Live[DeployedTopologyCompact]]) extends CommandResponseForJson

  implicit val commandResponseEncoder: Encoder[CommandResponse] = {
    def entityStates[E <: Entity](s: CommandResponse.StateListBase) = s.asInstanceOf[CommandResponse.StateList[E]].states
    def entityState[E <: Entity](s: CommandResponse.StateBase) = s.asInstanceOf[CommandResponse.State[E]].state
    Encoder.instance {
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[Topology] => TopologyForJson(entityState[Topology](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[Topology] => TopologiesForJson(entityStates[Topology](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[TopologyCompact] => TopologyCompactForJson(entityState[TopologyCompact](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[TopologyCompact] => TopologiesCompactForJson(entityStates[TopologyCompact](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[KafkaCluster] => KafkaClusterForJson(entityState[KafkaCluster](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[KafkaCluster] => KafkaClustersForJson(entityStates[KafkaCluster](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[Permission] => PermissionForJson(entityState[Permission](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[Permission] => PermissionsForJson(entityStates[Permission](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[Deployment] => DeploymentForJson(entityState[Deployment](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[Deployment] => DeploymentsForJson(entityStates[Deployment](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[DeployedTopology] => DeployedTopologyForJson(entityState[DeployedTopology](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[DeployedTopology] => DeployedTopologiesForJson(entityStates[DeployedTopology](s)).asJson
      case s: CommandResponse.StateBase if s.entityType =:= typeOf[DeployedTopologyCompact] => DeployedTopologyCompactForJson(entityState[DeployedTopologyCompact](s)).asJson
      case s: CommandResponse.StateListBase if s.entityType =:= typeOf[DeployedTopologyCompact] => DeployedTopologiesCompactForJson(entityStates[DeployedTopologyCompact](s)).asJson
      case t: CommandResponse.ResolvedTopologies => t.asJson
      case t: CommandResponse.ResolvedDeployedTopologies => t.asJson
      case t: CommandResponse.TopicIds => t.asJson
      case t: CommandResponse.ApplicationIds => t.asJson
      case t: CommandResponse.DeployedTopic => t.asJson
      case t: CommandResponse.DeployedApplication => t.asJson
      case t: CommandResponse.DeployedApplicationStatus => t.asJson
      case t: CommandResponse.DeployedTopologiesMetricsResponse => t.asJson
      case t: CommandResponse.DeployedTopologiesMetricsCompactResponse => t.asJson
      case t: CommandResponse.DeployedTopologyMetricsResponse => t.asJson
      case t: CommandResponse.DeployedTopologyMetricsCompactResponse => t.asJson
      case r: CommandResponse.FromKafkaCluster => r.kafkaClusterResponse.asJson
      case _: CommandResponse.None => Json.Null
    }
  }
  implicit val commandResponseDecoder: Decoder[CommandResponse] = {
    List[Decoder[CommandResponse]](
      Decoder[TopologyForJson].map(_.topology).map(CommandResponse.State[Topology]).widen,
      Decoder[TopologiesForJson].map(_.topologies).map(CommandResponse.StateList[Topology]).widen,
      Decoder[TopologyCompactForJson].map(_.topology).map(CommandResponse.State[TopologyCompact]).widen,
      Decoder[TopologiesCompactForJson].map(_.topologies).map(CommandResponse.StateList[TopologyCompact]).widen,
      Decoder[KafkaClusterForJson].map(_.kafkaCluster).map(CommandResponse.State[KafkaCluster]).widen,
      Decoder[KafkaClustersForJson].map(_.kafkaClusters).map(CommandResponse.StateList[KafkaCluster]).widen,
      Decoder[PermissionForJson].map(_.permission).map(CommandResponse.State[Permission]).widen,
      Decoder[PermissionsForJson].map(_.permissions).map(CommandResponse.StateList[Permission]).widen,
      Decoder[DeploymentForJson].map(_.deployment).map(CommandResponse.State[Deployment]).widen,
      Decoder[DeploymentsForJson].map(_.deployments).map(CommandResponse.StateList[Deployment]).widen,
      Decoder[DeployedTopologyForJson].map(_.deployedTopology).map(CommandResponse.State[DeployedTopology]).widen,
      Decoder[DeployedTopologiesForJson].map(_.deployedTopologies).map(CommandResponse.StateList[DeployedTopology]).widen,
      Decoder[DeployedTopologyCompactForJson].map(_.deployedTopology).map(CommandResponse.State[DeployedTopologyCompact]).widen,
      Decoder[DeployedTopologiesCompactForJson].map(_.deployedTopologies).map(CommandResponse.StateList[DeployedTopologyCompact]).widen,
      Decoder[CommandResponse.ResolvedTopologies].widen,
      Decoder[CommandResponse.ResolvedDeployedTopologies].widen,
      // this may not work, but doesn't matter, we don't decode these right now
      Decoder[KafkaClusterCommandResponse].map(CommandResponse.FromKafkaCluster).widen,
      // not decoding the TopicIds, ApplicationIds, DeployedTopic, DeployedApplication, DeployedApplicationStatus, DeployedTopologiesMetricsResponse, DeployedTopologiesMetricsCompactResponse at all,
      // not needed and ambiguous anyway
      Decoder.decodeNone.map(_ => CommandResponse.None()).widen,
    ).reduceLeft(_ or _)
  }

  // command results
  implicit val commandErrorTypeEncoder: Encoder[CommandErrorType] = deriveEnumerationEncoder
  implicit val commandErrorTypeDecoder: Decoder[CommandErrorType] = deriveEnumerationDecoder
  implicit val commandResultEncoder: Encoder[CommandResult] = deriveConfiguredEncoder
  implicit val commandResultDecoder: Decoder[CommandResult] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandActionSafetyEncoder: Encoder[KafkaClusterCommandActionSafety] = deriveEnumerationEncoder
  implicit val kafkaClusterCommandActionSafetyDecoder: Decoder[KafkaClusterCommandActionSafety] = deriveEnumerationDecoder
  implicit val kafkaClusterCommandResultEncoder: Encoder[KafkaClusterCommandResult] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResultDecoder: Decoder[KafkaClusterCommandResult] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandResponseTopicIdsEncoder: Encoder[KafkaClusterCommandResponse.TopicIds] = Encoder[Seq[String]].contramap(_.ids.map(_.id))
  implicit val kafkaClusterCommandResponseTopicIdsDecoder: Decoder[KafkaClusterCommandResponse.TopicIds] = Decoder[Seq[String]].map(ids => KafkaClusterCommandResponse.TopicIds(ids.map(LocalTopicId)))
  implicit val kafkaClusterCommandResponseApplicationIdsEncoder: Encoder[KafkaClusterCommandResponse.ApplicationIds] = Encoder[Seq[String]].contramap(_.ids.map(_.id))
  implicit val kafkaClusterCommandResponseApplicationIdsDecoder: Decoder[KafkaClusterCommandResponse.ApplicationIds] = Decoder[Seq[String]].map(ids => KafkaClusterCommandResponse.ApplicationIds(ids.map(LocalApplicationId)))

  implicit val kafkaClusterCommandResponseDiffResultTopicsEncoder: Encoder[KafkaClusterCommandResponse.DiffResult.Topics] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResponseDiffResultTopicsDecoder: Decoder[KafkaClusterCommandResponse.DiffResult.Topics] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandResponseDiffResultTopicsDifferenceEncoder: Encoder[KafkaClusterCommandResponse.DiffResult.Topics.Difference] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResponseDiffResultTopicsDifferenceDecoder: Decoder[KafkaClusterCommandResponse.DiffResult.Topics.Difference] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandResponseDiffResultAclEncoder: Encoder[KafkaClusterCommandResponse.DiffResult.Acl] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResponseDiffResultAclDecoder: Decoder[KafkaClusterCommandResponse.DiffResult.Acl] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandResponseDiffResultAclsEncoder: Encoder[KafkaClusterCommandResponse.DiffResult.Acls] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResponseDiffResultAclsDecoder: Decoder[KafkaClusterCommandResponse.DiffResult.Acls] = deriveConfiguredDecoder
  implicit val kafkaClusterCommandResponseDiffResultEncoder: Encoder[KafkaClusterCommandResponse.DiffResult] = deriveConfiguredEncoder
  implicit val kafkaClusterCommandResponseDiffResultDecoder: Decoder[KafkaClusterCommandResponse.DiffResult] = deriveConfiguredDecoder

  // entity state changes for all-state entities and deployment entities
  implicit val allStateEntityChangesEncoder: Encoder[AllStateEntitiesStateChanges] = deriveConfiguredEncoder
  implicit val allStateEntityChangesTypeDecoder: Decoder[AllStateEntitiesStateChanges] = deriveConfiguredDecoder
  implicit val allDeploymentEntityChangesEncoder: Encoder[AllDeploymentEntitiesStateChanges] = deriveConfiguredEncoder
  implicit val allDeploymentEntityChangesTypeDecoder: Decoder[AllDeploymentEntitiesStateChanges] = deriveConfiguredDecoder

  // ... and their transactions
  implicit val allStateEntityChangesTransactionItemEncoder: Encoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]] = deriveConfiguredEncoder
  implicit val allStateEntityChangesTransactionItemDecoder: Decoder[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]] = deriveConfiguredDecoder
  implicit val allDeploymentEntityChangesTransactionItemEncoder: Encoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]] = deriveConfiguredEncoder
  implicit val allDeploymentEntityChangesTransactionItemDecoder: Decoder[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]] = deriveConfiguredDecoder

  // ... and the change-log message
  implicit val kafkaChangeLogStoreMessageEncoder: Encoder[ChangeLogMessage] = deriveConfiguredEncoder
  implicit val kafkaChangeLogStoreMessageDecoder: Decoder[ChangeLogMessage] = deriveConfiguredDecoder
}
