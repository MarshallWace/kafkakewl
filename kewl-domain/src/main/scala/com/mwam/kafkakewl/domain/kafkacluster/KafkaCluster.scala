/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafkacluster

import com.mwam.kafkakewl.domain.topology.ReplicaPlacementId

final case class KafkaClusterEntityId(id: String) extends AnyVal with EntityId

/**
  * An entity type representing a kafka-cluster.
  *
  * @param kafkaCluster the kafka-cluster id (only so that the json can specify it)
  * @param brokers the list of brokers
  * @param zooKeeper the optional list of the zookeeper hosts (to avoid creating topics if they are pending-deleted in the '/admin/delete_topics')
  * @param zooKeeperRetryIntervalMs the zookeeper connection retry interval milliseconds, defaulting to 3000
  * @param securityProtocol the optional security protocol (if none, the default is used which is PLAINTEXT)
  * @param kafkaClientConfig the optional kafka client config that overrides everything else
  * @param name the optional human readable name of the kafka cluster
  * @param environments the list of environments associated with the kafka cluster (their order matters in the variable resolution)
  * @param nonKewl the non-kewl resources in this cluster
  * @param requiresAuthorizationCode true if any change requires an authorization code
  * @param replicaPlacementConfigs the replica placement configs by their identifier
  * @param defaultReplicaPlacementId the default replica placement or none, if there isn't any default (e.g. when replicaPlacements is empty)
  * @param topicConfigKeysAllowedInTopologies the topic config constraints that are allowed in topologies' topics
  * @param additionalManagedTopicConfigKeys the additional topic config constraints that are managed by kafkakewl (everything else will be ignored and left as it is in the kafka topic)
  * @param systemTopicsReplicaPlacementId the optional replica placement for system topics (e.g. the deployment state storage topic)
  * @param kafkaRequestTimeOutMillis the "request.timeout.ms" value for this kafka-cluster to use for deployment/reset operations.
  * @param tags the optional list of tags
  * @param labels the optional labels
  */
final case class KafkaCluster(
  kafkaCluster: KafkaClusterEntityId, // the only reason of this being here is that the input json can tell the kafka-cluster id
  brokers: String,
  zooKeeper: Option[String] = None,
  zooKeeperRetryIntervalMs: Option[Int] = None,
  securityProtocol: Option[String] = None,
  kafkaClientConfig: Map[String, String] = Map.empty,
  name: String = "",
  environments: DeploymentEnvironments.OrderedVariables = DeploymentEnvironments.OrderedVariables.empty,
  nonKewl: NonKewlKafkaResources = NonKewlKafkaResources(),
  requiresAuthorizationCode: Boolean = false,
  replicaPlacementConfigs: ReplicaPlacements = emptyReplicaPlacements,
  defaultReplicaPlacementId: Option[ReplicaPlacementId] = None,
  topicConfigKeysAllowedInTopologies: TopicConfigKeyConstraintInclusive = TopicConfigKeyConstraintInclusive(),
  additionalManagedTopicConfigKeys: TopicConfigKeyConstraintExclusive = TopicConfigKeyConstraintExclusive(),
  systemTopicsReplicaPlacementId: Option[ReplicaPlacementId] = None,
  kafkaRequestTimeOutMillis: Option[Int] = None,
  tags: Seq[String] = Seq.empty,
  labels: Map[String, String] = Map.empty
) extends Entity with Labelled {
  def toShortString = s"${securityProtocol.getOrElse("PLAINTEXT")}/$brokers/(${kafkaClientConfig.map(kv => s"${kv._1}=${kv._2}").mkString("(", ";", ")")})"

  def resolveTopicConfig(replicaPlacementId: Option[ReplicaPlacementId], topicConfig: Map[String, String]): Map[String, String] = {
    replicaPlacementId.orElse(defaultReplicaPlacementId)
      .flatMap(replicaPlacementConfigs.get)
      .map(applyTopicConfigDefaults(topicConfig, _))
      .getOrElse(topicConfig)
  }

  def resolveSystemTopicConfig(topicConfig: Map[String, String]): Map[String, String] = {
    systemTopicsReplicaPlacementId
      .flatMap(replicaPlacementConfigs.get)
      .map(applyTopicConfigDefaults(topicConfig, _))
      .getOrElse(topicConfig)
  }

  def isTopicConfigKeyAllowedInTopologies(topicConfigKey: String): Boolean = topicConfigKeysAllowedInTopologies.isIncluded(topicConfigKey)
  def isTopicConfigManaged(topicConfigKey: String): Boolean = isTopicConfigKeyAllowedInTopologies(topicConfigKey) || additionalManagedTopicConfigKeys.isIncluded(topicConfigKey)
}

object KafkaClusterStateChange {
  sealed trait StateChange extends SimpleEntityStateChange[KafkaCluster]
  final case class NewVersion(metadata: EntityStateMetadata, entity: KafkaCluster) extends StateChange with SimpleEntityStateChange.NewVersion[KafkaCluster]
  final case class Deleted(metadata: EntityStateMetadata) extends StateChange with SimpleEntityStateChange.Deleted[KafkaCluster]
}
