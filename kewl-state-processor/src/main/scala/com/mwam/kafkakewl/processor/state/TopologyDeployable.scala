/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.apply._
import cats.syntax.option._
import cats.syntax.validated._
import cats.syntax.traverse._
import cats.syntax.either._
import cats.instances.vector._
import cats.instances.option._
import cats.data.Validated._
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.domain.{CommandError, DeploymentEnvironment, DeploymentEnvironments, Expressions}
import com.mwam.kafkakewl.utils._

object TopologyDeployable {
  type MakeDeployableResult[T] = ValidatedNel[CommandError, T]

  implicit class ExpressionResultExtensions[T](r: Either[String, T]) {
    def toValidated: MakeDeployableResult[T] = r.leftMap(CommandError.validationError).toValidatedNel
  }

  implicit class ExpressionResultsExtensions[T](r: Either[Seq[String], T]) {
    def toValidated: MakeDeployableResult[T] = r.leftMap(e => NonEmptyList.fromListUnsafe(e.map(CommandError.validationError).toList)).toValidated
  }

  implicit class DeployableResultExtensions[T](r: MakeDeployableResult[T]) {
    def failuresOf[O](owner: => O): MakeDeployableResult[T] = r.leftMap(e => e.map(ce => ce.copy(message = s"$owner: ${ce.message}")))
  }

  def deploymentVariables(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster, topology: Topology): Expressions.Variables = {
    kafkaCluster
      .environments
      .map {
        case (e, v) =>
          // for kafka-cluster's environment: take the defaults in the code,
          DeploymentEnvironments.Variables.defaults.getOrElse(e, DeploymentEnvironment.Variables.empty) ++
            // then override them with the kafka-cluster's variables
            v ++
            // finally, override them with the variables from the topology itself
            topology.environments.getOrElse(e, DeploymentEnvironment.Variables.empty)
      }
      // from left to right, we combine these environment variables dictionaries into one
      .reduceLeft(_ ++ _)
      .mapValues(v => v.values)
  }

  def makeTopologyDeployable(variables: Expressions.Variables, topology: Topology): MakeDeployableResult[TopologyToDeploy] = {

    def makeTagsDeployable(tags: Topology.TagsExpr): MakeDeployableResult[Seq[String]] = {
      tags.map(_.value(variables).toValidated).toVector.sequence[MakeDeployableResult, String].map(_.filter(_.nonNullAndEmpty).toSeq)
    }

    def makeLabelsDeployable(labels: Topology.LabelsExpr): MakeDeployableResult[Map[String, String]] = {
      labels
        .map { case (k, v) => (k.value(variables).toValidated, v.value(variables).toValidated).tupled }
        .toVector
        .sequence[MakeDeployableResult, (String, String)]
        .map(_.toMap.filterKeys(_.nonNullAndEmpty))
    }

    def makeTopicConfigDeployable(topicConfig: Map[String, Topology.TopicConfigExpr]): MakeDeployableResult[Map[String, String]] = {
      topicConfig
        .map { case (k, v) => v.value(variables).toValidated.map(k -> _) }
        .toVector
        .sequence[MakeDeployableResult, (String, String)]
        .map(_.toMap.filter { case (_, v) => v.nonNullAndEmpty} )
    }

    def makeTopicDeployable(topology: Topology)(
      idAndTopic: (LocalTopicId, Topology.Topic)
    ): MakeDeployableResult[(LocalTopicId, TopologyToDeploy.Topic)] = {
      val (topicId, topic) = idAndTopic
      (
        topic.name.validNel,
        topic.unManaged.validNel,
        topic.partitions.value(variables).toValidated,
        topic.replicationFactor.map(_.value(variables).toValidated).sequence,
        makeTopicConfigDeployable(topic.config),
        topic.replicaPlacement.map(_.value(variables).toValidated).map(_.map(ReplicaPlacementId)).sequence, // need to turn the Option[Validated[...]] inside out to be able to do the mapN
        topic.description.validNel,
        topic.otherConsumerNamespaces.validNel,
        topic.otherProducerNamespaces.validNel,
        makeTagsDeployable(topic.tags),
        makeLabelsDeployable(topic.labels)
      ).mapN(TopologyToDeploy.Topic)
       .map(topicId -> _)
       .failuresOf(s"in topic ${topology.fullyQualifiedTopicId(topicId).quote}")
    }

    def makeApplicationTypeDeployable(
      applicationType: Topology.Application.Type
    ): MakeDeployableResult[TopologyToDeploy.Application.Type] = (applicationType match {
      case t: Topology.Application.Type.Simple => TopologyToDeploy.Application.Type.Simple(t.consumerGroup, t.transactionalId)
      case t: Topology.Application.Type.KafkaStreams => TopologyToDeploy.Application.Type.KafkaStreams(t.kafkaStreamsAppId)
      case t: Topology.Application.Type.Connector => TopologyToDeploy.Application.Type.Connector(t.connector)
      case t: Topology.Application.Type.ConnectReplicator => TopologyToDeploy.Application.Type.ConnectReplicator(t.connectReplicator)
    }).validNel

    def makeApplicationDeployable(topology: Topology)(
      idAndApplication: (LocalApplicationId, Topology.Application)
    ): MakeDeployableResult[(LocalApplicationId, TopologyToDeploy.Application)] = {
      val (applicationId, application) = idAndApplication
      (
        application.user.value(variables).toValidated,
        application.host.validNel,
        makeApplicationTypeDeployable(application.`type`),
        application.description.validNel,
        application.otherConsumableNamespaces.validNel,
        application.otherProducableNamespaces.validNel,
        application.consumerLagWindowSeconds
          .map(_.value(variables).map(_.some).toValidated)
          .getOrElse(none[Int].validNel),
        makeTagsDeployable(application.tags),
        makeLabelsDeployable(application.labels)
      ).mapN(TopologyToDeploy.Application.apply)
       .map(applicationId -> _)
       .failuresOf(s"in application ${topology.fullyQualifiedApplicationId(applicationId).quote}")
    }

    def makeRelationshipsDeployable(topology: Topology): MakeDeployableResult[TopologyToDeploy.Relationships] = {
      def makeNodeRelationshipsDeployable(relationshipProperties: Topology.RelationshipProperties): MakeDeployableResult[TopologyToDeploy.RelationshipProperties] = {
        (
          relationshipProperties.reset.validNel,
          relationshipProperties.monitorConsumerLag.validNel,
          makeTagsDeployable(relationshipProperties.tags),
          makeLabelsDeployable(relationshipProperties.labels)
        ).mapN(TopologyToDeploy.RelationshipProperties)
      }

      def makeNodeRelationshipPropertiesDeployable(nodeRelationshipProperties: Topology.NodeRelationshipProperties): MakeDeployableResult[TopologyToDeploy.NodeRelationshipProperties] = {
        (
          nodeRelationshipProperties.id.validNel,
          makeNodeRelationshipsDeployable(nodeRelationshipProperties.properties)
        ).mapN(TopologyToDeploy.NodeRelationshipProperties)
      }

      topology.relationships
        .map {
          case (nodeRef, relationships) =>
            relationships
              .map {
                case (relationshipType, nodeRelProps) =>
                  nodeRelProps
                    .map(makeNodeRelationshipPropertiesDeployable)
                    .toVector.sequence[MakeDeployableResult, TopologyToDeploy.NodeRelationshipProperties]
                    .map((relationshipType, _))
              }
              .toVector.sequence[MakeDeployableResult, (RelationshipType, Seq[TopologyToDeploy.NodeRelationshipProperties])]
              .map(n => (nodeRef, n.toMap))
        }
        .toVector.sequence[MakeDeployableResult, (NodeRef, TopologyToDeploy.NodeRelationships)]
        .map(_.toMap)
        .failuresOf("in relationships")
    }

    (
      topology.namespace.validNel,
      topology.topology.validNel,
      topology.description.validNel,
      topology.developers.validNel,
      topology.developersAccess.value(variables).toValidated,
      topology.deployWithAuthorizationCode.value(variables).toValidated.map(_.some),
      topology.topics.map(makeTopicDeployable(topology)).toVector.sequence[MakeDeployableResult, (LocalTopicId, TopologyToDeploy.Topic)].map(_.toMap),
      topology.applications.map(makeApplicationDeployable(topology)).toVector.sequence[MakeDeployableResult, (LocalApplicationId, TopologyToDeploy.Application)].map(_.toMap),
      topology.aliases.validNel,
      makeRelationshipsDeployable(topology),
      makeTagsDeployable(topology.tags),
      makeLabelsDeployable(topology.labels)
    ).mapN(TopologyToDeploy.apply)
  }
}