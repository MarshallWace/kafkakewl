/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, TopologyToDeployWithVersion}
import com.mwam.kafkakewl.domain.kafkacluster.{IsTopicConfigManaged, KafkaClusterEntityId, ResolveTopicConfig, allTopicConfigsAreManaged, nullResolveReplicaPlacement}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.topology.{TopologyEntityId, TopologyToDeploy}
import org.scalatest.Matchers

trait DeployTopologyCommon {
  this: Matchers =>

  implicit class KafkaClusterItemMapExtensions(kafkaClusterItemsMap: Map[String, KafkaClusterItem]) {
    def without(items: Map[String, KafkaClusterItem]): Map[String, KafkaClusterItem] = kafkaClusterItemsMap.filterKeys(!items.keySet.contains(_))
  }

  def kafkaClusterItemsOf(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    isKafkaClusterSecurityEnabled: Boolean,
    resolveTopicConfig: ResolveTopicConfig = nullResolveReplicaPlacement,
    topicDefaults: TopicDefaults = TopicDefaults()
  ): Seq[KafkaClusterItem] =
    KafkaClusterItems.forAllTopologies(
      resolveTopicConfig,
      currentDeployedTopologies.map { case (_, dps) => (dps.entity.topologyId, dps.entity.topologyWithVersion.get.topology) },
      isKafkaClusterSecurityEnabled,
      topicDefaults
    ).map(_.kafkaClusterItem)

  def kafkaClusterItemsMapOf(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    isKafkaClusterSecurityEnabled: Boolean,
  ): Map[String, KafkaClusterItem] =
    kafkaClusterItemsOf(currentDeployedTopologies, isKafkaClusterSecurityEnabled).map(_.toTuple).toMap

  def kafkaClusterItemOfTopologiesOf(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    isKafkaClusterSecurityEnabled: Boolean,
    resolveTopicConfig: ResolveTopicConfig = nullResolveReplicaPlacement,
    topicDefaults: TopicDefaults = TopicDefaults()
  ): Seq[KafkaClusterItemOfTopology] =
    KafkaClusterItems.forAllTopologies(
      resolveTopicConfig,
      currentDeployedTopologies.map { case (_, dps) => (dps.entity.topologyId, dps.entity.topologyWithVersion.get.topology) },
      isKafkaClusterSecurityEnabled,
      topicDefaults
    )
      .filter(_.kafkaClusterItem.isReal)
      // from the kafka-cluster they come without owners
      .map(_.withOwnerTopologyIds(Set.empty))

  /**
    * This takes both the current deployed topologies and the kafka cluster items in the cluster as parameters.
    */
  def createDeployChanges(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    deployedKafkaClusterItems: Seq[KafkaClusterItemOfTopology],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy],
    resolveTopicConfig: ResolveTopicConfig = nullResolveReplicaPlacement,
    isTopicConfigManaged: IsTopicConfigManaged = allTopicConfigsAreManaged,
    topicDefaults: TopicDefaults = TopicDefaults()
  ): Map[String, KafkaClusterChange] =
    DeployTopology.createChangesToDeployTopology(
      resolveTopicConfig,
      isTopicConfigManaged,
      currentDeployedTopologies,
      currentDeployedTopologies.mapValues(_.entity.topologyWithVersion).collect { case (p, Some(pv)) => (p, pv.topology) },
      deployedKafkaClusterItems,
      isKafkaClusterSecurityEnabled,
      topologyId,
      topologyToDeployOrNone,
      topicDefaults
    ).map(_.toTuple).toMap

  /**
    * This assumes that the kafka cluster currently has the currentDeployedTopologies's kafka cluster items PLUS some additional ones.
    */
  def createDeployChangesWithRedundantKafkaClusterItems(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    additionalDeployedKafkaClusterItems: Seq[KafkaClusterItemOfTopology],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy]
  ): Map[String, KafkaClusterChange] =
    createDeployChanges(
      currentDeployedTopologies,
      kafkaClusterItemOfTopologiesOf(currentDeployedTopologies, isKafkaClusterSecurityEnabled) ++ additionalDeployedKafkaClusterItems,
      isKafkaClusterSecurityEnabled,
      topologyId,
      topologyToDeployOrNone
    )

  /**
    * This assumes that the kafka cluster currently has the currentDeployedTopologies's kafka cluster items but replacing some to the differentDeployedKafkaClusterItems.
    */
  def createDeployChangesWithDifferentKafkaClusterItems(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    differentDeployedKafkaClusterItems: Seq[KafkaClusterItemOfTopology],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy]
  ): Map[String, KafkaClusterChange] =
    createDeployChanges(
      currentDeployedTopologies,
      kafkaClusterItemOfTopologiesOf(currentDeployedTopologies, isKafkaClusterSecurityEnabled)
        .map(i => differentDeployedKafkaClusterItems.find(_.kafkaClusterItem.key == i.kafkaClusterItem.key).getOrElse(i)),
      isKafkaClusterSecurityEnabled,
      topologyId,
      topologyToDeployOrNone
    )

  /**
    * This assumes that the kafka cluster currently has the currentDeployedTopologies's kafka cluster items except for withoutDeployedKafkaClusterItems.
    */
  def createDeployChangesWithoutKafkaClusterItems(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    withoutDeployedKafkaClusterItems: Seq[KafkaClusterItemOfTopology],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy]
  ): Map[String, KafkaClusterChange] =
    createDeployChanges(
      currentDeployedTopologies,
      kafkaClusterItemOfTopologiesOf(currentDeployedTopologies, isKafkaClusterSecurityEnabled)
        .filter(i => !withoutDeployedKafkaClusterItems.exists(_.kafkaClusterItem.key == i.kafkaClusterItem.key)),
      isKafkaClusterSecurityEnabled,
      topologyId,
      topologyToDeployOrNone
    )

  /**
    * This assumes that the kafka cluster currently has the currentDeployedTopologies's kafka cluster items.
    */
  def createDeployChangesNoDiscrepancies(
    currentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]],
    isKafkaClusterSecurityEnabled: Boolean,
    topologyId: TopologyEntityId,
    topologyToDeployOrNone: Option[TopologyToDeploy]
  ): Map[String, KafkaClusterChange] = createDeployChanges(
    currentDeployedTopologies,
    kafkaClusterItemOfTopologiesOf(currentDeployedTopologies, isKafkaClusterSecurityEnabled),
    isKafkaClusterSecurityEnabled,
    topologyId,
    topologyToDeployOrNone
  )

  val noCurrentDeployedTopologies: Map[TopologyEntityId, EntityState.Live[DeployedTopology]] = Map.empty
  val kafkaClusterId = KafkaClusterEntityId("test-cluster")

  def deployed(
    topologyToDeploy: TopologyToDeploy,
    project: String = "test",
    deploymentVersion: Int = 1,
    topologyVersion: Int = 1,
    deployedTopologyVersion: Int = 1,
    notRemovedKeys: Seq[String] = Seq.empty
  ): (TopologyEntityId, EntityState.Live[DeployedTopology]) =
    (
      TopologyEntityId(project),
      DeployedTopology(
        kafkaClusterId,
        TopologyEntityId(project),
        deploymentVersion,
        Some(TopologyToDeployWithVersion(topologyVersion, topologyToDeploy)),
        allActionsSuccessful = true,
        notRemovedKeys
      ).toLiveState(deployedTopologyVersion, "test")
    )

  /**
    * We keep this true for all tests. The KafkaClusterItemsSpec does test this with false, that's enough.
    */
  val isKafkaClusterSecurityEnabled = true
}
