/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.validation

import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.domain.topology._
import com.mwam.kafkakewl.utils._

import scala.reflect.ClassTag

class TopologyLikeValidatorStandalone[NodeType, TopicType <: TopologyLike.Topic with NodeType : ClassTag, ApplicationType <: TopologyLike.Application with NodeType : ClassTag, RelationshipPropertiesType <: TopologyLike.RelationshipProperties]
  extends ValidationUtils {

  type TopologyType = TopologyLike[ApplicationType, TopicType, RelationshipPropertiesType]

  def validateTopologyNamespacePart(namespacePart: String, partName: String): Result =
    Validation.Result.validationErrorIf(
      namespacePart != "" && """^[a-zA-Z0-9\-_]+(\.([a-zA-Z0-9\-_]+))*$""".r.findFirstIn(namespacePart).isEmpty,
      s"$partName ${namespacePart.quote} must not start or end with '.', '_', '_' and can contain only alphanumeric characters and '.', '-', '_'"
    )

  def validateTopologyNodeId(nodeId: String, nodeType: String): Result =
    Validation.Result.validationErrorIf(
      """^[a-zA-Z0-9\-_]+$""".r.findFirstIn(nodeId).isEmpty,
      s"$nodeType ${nodeId.quote} can contain only alphanumeric characters and '-', '_'"
    )

  def validateTopologyNamespace(namespace: Namespace): Result = validateTopologyNamespacePart(namespace.ns, "namespace")
  def validateTopologyTopology(topologyId: TopologyId): Result = validateTopologyNamespacePart(topologyId.id, "topology id")
  def validateTopologyTopicId(topicId: LocalTopicId): Result = validateTopologyNodeId(topicId.id, "topic id")
  def validateTopologyApplicationId(applicationId: LocalApplicationId): Result = validateTopologyNodeId(applicationId.id, "application id")
  def validateTopologyTopicAliasId(aliasId: LocalAliasId): Result = validateTopologyNodeId(aliasId.id, "topic alias id")
  def validateTopologyApplicationAliasId(aliasId: LocalAliasId): Result = validateTopologyNodeId(aliasId.id, "application alias id")

  def validateTopologyTopic(topic: TopicType): Result = {
    Seq(
      Validation.Result.validationErrorIf(
        """^[a-zA-Z0-9\.\-_]*$""".r.findFirstIn(topic.name).isEmpty,
        s"topic name '${topic.name}' contains invalid characters, only alphanumeric characters and '.', '-', '_' are allowed"
      ),
      topic.name.validateShorterThan(249, "topic name"),
      topic.name.validateNonEmpty("topic name")
    ).combine()
  }

  def validateTopologyApplication(application: ApplicationType): Result = {
    Seq(
      application.simpleConsumerGroup.map(_.validateNonEmpty(s"application '$application''s consumer group")).getOrElse(Validation.Result.success),
      application.kafkaStreamsAppId.map(_.validateNonEmpty(s"application '$application''s kafka streams app-id")).getOrElse(Validation.Result.success),
      application.connector.map(_.validateNonEmpty(s"application '$application''s connector")).getOrElse(Validation.Result.success),
      application.connectReplicator.map(_.validateNonEmpty(s"application '$application''s connect replicator")).getOrElse(Validation.Result.success),

      Validation.Result.validationErrorIf(application.isConnector && application.otherConsumableNamespaces.nonEmpty, s"connect application cannot have other consumable namespaces"),
      Validation.Result.validationErrorIf(application.isConnector && application.otherProducableNamespaces.nonEmpty, s"connect application cannot have other producable namespaces"),
      Validation.Result.validationErrorIf(application.isConnectReplicator && application.otherConsumableNamespaces.nonEmpty, s"connect replicator application cannot have other consumable namespaces"),
      Validation.Result.validationErrorIf(application.isConnectReplicator && application.otherProducableNamespaces.nonEmpty, s"connect replicator application cannot have other producable namespaces"),

      Validation.Result.validationErrorIf(application.isSimple && application.simpleConsumerGroup.isEmpty && application.otherConsumableNamespaces.nonEmpty, s"simple application without a consumer group cannot have other consumable namespaces")
    ).combine()
  }

  def validateStandaloneTopology(topologyId: TopologyEntityId, topology: TopologyType): Validation.Result = {
    def isPartOfNamespace(name: String): Boolean = topology.namespace.contains(name)

    val namespace = topology.namespace

    // topics and applications must be part of the namespace
    val topicsNotPartOfNamespace = topology.fullyQualifiedTopics.values.map(_.name).filterNot(isPartOfNamespace)
    val kafkaStreamsAppIdsNotPartOfNamespace = topology.fullyQualifiedApplications.values.flatMap(_.kafkaStreamsAppId).filterNot(isPartOfNamespace)
    val consumerGroupsNotPartOfNamespace = topology.fullyQualifiedApplications.values.flatMap(_.simpleConsumerGroup).filterNot(isPartOfNamespace)
    val transactionalIdsNotPartOfNamespace = topology.fullyQualifiedApplications.values.flatMap(_.simpleTransactionalId).filterNot(isPartOfNamespace)
    // no similar rules for connect and connect-replicator applications for now

    val topicIds = topology.fullyQualifiedTopics.keys.map(_.id).toSeq
    val topicAliasIds = topology.fullyQualifiedAliases.topics.keys.map(_.id).toSeq
    val allTopicIds = topicIds ++ topicAliasIds

    val applicationIds = topology.fullyQualifiedApplications.keys.map(_.id).toSeq
    val applicationAliasIds = topology.fullyQualifiedAliases.applications.keys.map(_.id).toSeq
    val allApplicationIds = applicationIds ++ applicationAliasIds

    val allNodeIds = allTopicIds ++ allApplicationIds

    // duplicate topics are not allowed across the defined topics (not checking the references, that's done in the TopologyValidatorWithOthers)
    val duplicateTopicNames = topology.fullyQualifiedTopics.values.map(_.name).duplicates
    // duplicate topic ids are not allowed across the defined topics of this topology and its topic aliases
    val duplicateTopicIds = allTopicIds.duplicates
    val duplicateTopicIdsSet = duplicateTopicIds.toSet
    // duplicate application ids are not allowed across the defined applications of this topology and its application aliases
    val duplicateApplicationIds = allApplicationIds.duplicates
    val duplicateApplicationIdsSet = duplicateApplicationIds.toSet
    // duplicate topic and application ids across everything (except the previous duplicates to avoid duplicating validation errors)
    val duplicateIds = allNodeIds.duplicates.filter(d => !duplicateTopicIdsSet(d) && !duplicateApplicationIdsSet(d))
    //    // duplicate group names are also not allowed for these applications (technically we could though)
    //    val duplicateGroups = topology.applications.values.flatMap(_.actualConsumerGroup).duplicates
    //    // duplicate transactional ids are also not allowed for these applications
    //    val duplicateTransactionalIds = topology.applications.values.flatMap(_.actualTransactionalId).duplicates
    //        // filter for the ones which present as explicitly specified transactional-id (the rest if covered by the duplicate-groups for kafka-streams)
    //        .filter(d => topology.applications.values.exists(_.typeAsSimple.flatMap(_.transactionalId).contains(d)))

    Seq(
      validateTopologyNamespace(namespace),
      validateTopologyTopology(topology.topology),
      topology.topics.keys.map(validateTopologyTopicId).combine(),
      topology.applications.keys.map(validateTopologyApplicationId).combine(),
      topology.aliases.topics.keys.map(validateTopologyTopicAliasId).combine(),
      topology.aliases.applications.keys.map(validateTopologyApplicationAliasId).combine(),
      Validation.Result.validationErrorIf(topology.topologyEntityId.isEmpty, s"at least one of the namespace or topology must be specified"),
      Validation.Result.validationErrorIf(!topology.topologyEntityId.isEmpty && topology.topologyEntityId != topologyId, s"the topology's id (${topology.topologyEntityId.quote}) cannot be different from ${topologyId.quote}"),
      Validation.Result.validationErrorIf(topicsNotPartOfNamespace.nonEmpty, s"topic names ${topicsNotPartOfNamespace.map(_.quote).mkString(", ")} are not part of namespace ${namespace.quote}"),
      Validation.Result.validationErrorIf(consumerGroupsNotPartOfNamespace.nonEmpty, s"consumer groups ${consumerGroupsNotPartOfNamespace.map(_.quote).mkString(", ")} are not part of namespace ${namespace.quote}"),
      Validation.Result.validationErrorIf(transactionalIdsNotPartOfNamespace.nonEmpty, s"transactional ids ${transactionalIdsNotPartOfNamespace.map(_.quote).mkString(", ")} are not part of namespace ${namespace.quote}"),
      Validation.Result.validationErrorIf(kafkaStreamsAppIdsNotPartOfNamespace.nonEmpty, s"kafka streams application ids ${kafkaStreamsAppIdsNotPartOfNamespace.map(_.quote).mkString(", ")} are not part of namespace ${namespace.quote}"),
      Validation.Result.validationErrorIf(duplicateTopicNames.nonEmpty, s"duplicate topic names: ${duplicateTopicNames.map(_.quote).mkString(", ")}"),
      Validation.Result.validationErrorIf(duplicateTopicIds.nonEmpty, s"duplicate topic ids: ${duplicateTopicIds.map(_.quote).mkString(", ")}"),
      Validation.Result.validationErrorIf(duplicateApplicationIds.nonEmpty, s"duplicate application ids: ${duplicateApplicationIds.map(_.quote).mkString(", ")}"),
      Validation.Result.validationErrorIf(duplicateIds.nonEmpty, s"duplicate ids: ${duplicateIds.map(_.quote).mkString(", ")}"),
      //      for now we allow duplicate consumer groups and transactional ids so that we can have different "logical" applications using them
      //      Validation.Result.validationErrorIf(duplicateGroups.nonEmpty, s"duplicate consumer groups: ${duplicateGroups.map(_.quote).mkString(", ")}"),
      //      Validation.Result.validationErrorIf(duplicateTransactionalIds.nonEmpty, s"duplicate transactional ids: ${duplicateTransactionalIds.map(_.quote).mkString(", ")}"),
      topology.fullyQualifiedTopics.values.map(validateTopologyTopic).combine(),
      topology.fullyQualifiedApplications.values.map(validateTopologyApplication).combine()
    ).combine()
  }
}
