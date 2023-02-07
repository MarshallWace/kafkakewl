/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import com.mwam.kafkakewl.domain.topology.TopologyEntityId
import com.mwam.kafkakewl.domain.KafkaClusterItemEntityType
import org.apache.kafka.common.acl.{AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourceType}

/**
  * Contains a KafkaClusterItem with its owner topology ids (for cross-topology ACLs there may be 2)
  *
  * There are other cases when there are more than one owner topology ids: e.g. when multiple topologies
  * contain a topic or ACL in a should-be-removed state, but it wasn't actually removed, because it was unsafe to do so.
  *
  * It's totally fine to have more than 1 owner topology ids, the sole purpose of this is to be able to tell
  * whether a KafkaClusterItem belongs to the currently being deployed topology or others (or no topology at all).
  *
  * @param kafkaClusterItem the kafka cluster item
  * @param ownerTopologyIds the owner topology ids
  */
private[kafkacluster] final case class KafkaClusterItemOfTopology(
  kafkaClusterItem: KafkaClusterItem,
  ownerTopologyIds: Set[TopologyEntityId]
) {
  def withOwnerTopologyIds(ownerTopologyIds: Set[TopologyEntityId]): KafkaClusterItemOfTopology = copy(ownerTopologyIds = ownerTopologyIds)

  /**
    * Assigns owner topology ids based on topologyIdsOfKafkaClusterItemKeys if it's empty in here.
    *
    * @param topologyIdsOfKafkaClusterItemKeys the topology ids of KafkaClusterItems
    * @return a KafkaClusterItemOfTopology with owner topology ids assigned, if possible.
    */
  def assignOwnerTopologyIdsIfMissing(topologyIdsOfKafkaClusterItemKeys: Map[String, Set[TopologyEntityId]]): KafkaClusterItemOfTopology =
    if (ownerTopologyIds.isEmpty)
      topologyIdsOfKafkaClusterItemKeys.get(kafkaClusterItem.key).map(withOwnerTopologyIds).getOrElse(this)
    else
      this

  /**
    * Returns true if the specified topology id is an owner of this KafkaClusterItem
    *
    * @param ownerTopologyId the topology id in question
    * @return true if the specified topology id is an owner of this KafkaClusterItem
    */
  def hasOwnerTopology(ownerTopologyId: TopologyEntityId): Boolean = ownerTopologyIds.contains(ownerTopologyId)

  def toTuple: (String, KafkaClusterItemOfTopology) = (kafkaClusterItem.key, this)
}

object KafkaClusterItemOfTopology {
  def apply(kafkaClusterItem: KafkaClusterItem, ownerTopologyId: TopologyEntityId): KafkaClusterItemOfTopology =
    new KafkaClusterItemOfTopology(kafkaClusterItem, Set(ownerTopologyId))

  def apply(kafkaClusterItem: KafkaClusterItem): KafkaClusterItemOfTopology =
    new KafkaClusterItemOfTopology(kafkaClusterItem, Set.empty)
}

/**
  * Base trait of all kafka cluster items (which are really just topics and ACLs).
  */
private[kafkacluster] sealed trait KafkaClusterItem {
  def key: String
  def isReal: Boolean = true
  def entityType: KafkaClusterItemEntityType
  def withOwnerTopologyId(ownerTopologyId: TopologyEntityId) = KafkaClusterItemOfTopology(this, ownerTopologyId)
  def withOwnerTopologyIds(ownerTopologyIds: Set[TopologyEntityId]) = KafkaClusterItemOfTopology(this, ownerTopologyIds)
  def withoutOwnerTopologyIds() = KafkaClusterItemOfTopology(this, Set.empty[TopologyEntityId])
  def toTuple: (String, KafkaClusterItem) = (key, this)
}
private[kafkacluster] object KafkaClusterItem {
  /**
    * A kafka-cluster item to represent a kafka topic.
    *
    * @param isReal true if it's an actual topic that should exist in the cluster or false if it's an un-managed topic which may or may not exist
    */
  final case class Topic(
    name: String,
    partitions: Int = 1,
    replicationFactor: Short = 3,
    config: Map[String, String],
    override val isReal: Boolean = true
  ) extends KafkaClusterItem {

    /**
      * The name of the topic is its key (you can't have more than one topic with the same name).
      */
    def key: String = s"topic:$name"
    def entityType: KafkaClusterItemEntityType = KafkaClusterItemEntityType.Topic

    def withPartitions(partitions: Int): Topic = copy(partitions = partitions)
    def withReplicationFactor(replicationFactor: Short): Topic = copy(replicationFactor = replicationFactor)
    def withConfig(key: String, value: String): Topic = copy(config = config + (key -> value))
    def withoutConfig(key: String): Topic = copy(config = config - key)

    override def toString: String = s"$name, partitions=$partitions, replicationFactor=$replicationFactor, config=$config"
  }

  /**
    * A kafka cluster item to represent a kafka ACL.
    */
  final case class Acl(
    resourceType: ResourceType,
    resourcePatternType: PatternType,
    resourceName: String,
    principal: String,
    host: String,
    operation: AclOperation,
    permission: AclPermissionType
  ) extends KafkaClusterItem {
    /**
      * The key of the ACL is itself with all its fields. It's not possible to "update" and ACL, only add or remove.
      */
    def key: String = s"acl:$resourceType/$resourcePatternType/$resourceName/$principal/$host/$operation/$permission"
    def entityType: KafkaClusterItemEntityType = KafkaClusterItemEntityType.Acl

    override def toString: String = s"$permission $operation for $principal on host $host for $resourcePatternType $resourceType $resourceName"
  }

  /**
    * The result of a diff-operation between two sets of KafkaClusterItemOfTopologies.
    *
    * @param missingFrom1 the items missing from the first set
    * @param missingFrom2 the items missing from the second set
    * @param changes the items that are different between the two sets
    */
  final case class DiffResult(
    missingFrom1: Seq[KafkaClusterItemOfTopology],
    missingFrom2: Seq[KafkaClusterItemOfTopology],
    changes: Seq[(KafkaClusterItemOfTopology, KafkaClusterItemOfTopology)]
  ) {
    /**
      * Tries to assign topologyIds to KafkaClusterItemOfTopologies which don't have any
      * based on topologyIdsOfKafkaClusterItemKeys.
      *
      * @param topologyIdsOfKafkaClusterItemKeys a map containing owner topology ids for KafkaClusterItem keys
      * @return a DiffResult instance with hopefully more ownerTopologyIds than before.
      */
    def assignOwnerTopologyIdsIfMissing(
      topologyIdsOfKafkaClusterItemKeys: Map[String, Set[TopologyEntityId]]
    ): DiffResult = {

      DiffResult(
        missingFrom1.map(_.assignOwnerTopologyIdsIfMissing(topologyIdsOfKafkaClusterItemKeys)),
        missingFrom2.map(_.assignOwnerTopologyIdsIfMissing(topologyIdsOfKafkaClusterItemKeys)),
        changes.map { case (first, second) =>
            assert(first.kafkaClusterItem.key == second.kafkaClusterItem.key)
            if (first.ownerTopologyIds.isEmpty && second.ownerTopologyIds.isEmpty) {
              // neither has owner topology ids => they must have the same key, so we try to find owners for that and assign to both
              val topologyOwnerIds = topologyIdsOfKafkaClusterItemKeys.getOrElse(first.kafkaClusterItem.key, Set.empty)
              (first.withOwnerTopologyIds(topologyOwnerIds), second.withOwnerTopologyIds(topologyOwnerIds))
            } else if (first.ownerTopologyIds.nonEmpty && second.ownerTopologyIds.isEmpty) {
              // one has owners, the other doesn't: assign the owners to the other
              (first, second.withOwnerTopologyIds(first.ownerTopologyIds))
            } else if (first.ownerTopologyIds.isEmpty && second.ownerTopologyIds.nonEmpty) {
              // one has owners, the other doesn't: assign the owners to the other
              (first.withOwnerTopologyIds(second.ownerTopologyIds), second)
            } else {
              // they both have owners, nothing to do (the owners must be the same, otherwise something is really wrong)
              assert(first.ownerTopologyIds == second.ownerTopologyIds)
              (first, second)
            }
        }
      )
    }

    def partitionByOwnerTopologyId(ownerTopologyId: TopologyEntityId): (DiffResult, DiffResult) = {
      val (missingFrom1OfTopology, missingFrom1OfOthers) = missingFrom1.partition(_.hasOwnerTopology(ownerTopologyId))
      val (missingFrom2OfTopology, missingFrom2OfOthers) = missingFrom2.partition(_.hasOwnerTopology(ownerTopologyId))
      val (changesOfTopology, changesOfOthers) = changes.partition { case (first, second) =>
        assert(first.kafkaClusterItem.key == second.kafkaClusterItem.key)
        assert(first.ownerTopologyIds == second.ownerTopologyIds)
        first.hasOwnerTopology(ownerTopologyId)
      }
      (
        DiffResult(missingFrom1OfTopology, missingFrom2OfTopology, changesOfTopology),
        DiffResult(missingFrom1OfOthers, missingFrom2OfOthers, changesOfOthers)
      )
    }

    def missingFrom1AsKafkaClusterItems: Seq[KafkaClusterItem] = missingFrom1.map(_.kafkaClusterItem)
    def missingFrom2AsKafkaClusterItems: Seq[KafkaClusterItem] = missingFrom2.map(_.kafkaClusterItem)
    def changesAsKafkaClusterItems: Seq[(KafkaClusterItem, KafkaClusterItem)] = changes.map { case (first, second) => (first.kafkaClusterItem, second.kafkaClusterItem) }

    def toKafkaClusterChanges: Map[String, KafkaClusterChange] = {
      val kafkaClusterRemoves = KafkaClusterChanges.remove(missingFrom2AsKafkaClusterItems)
      val kafkaClusterUpdates = KafkaClusterChanges.update(changesAsKafkaClusterItems)
      val kafkaClusterAdds = KafkaClusterChanges.add(missingFrom1AsKafkaClusterItems)
      (kafkaClusterAdds ++ kafkaClusterUpdates ++ kafkaClusterRemoves).map(c => (c.key, c)).toMap
    }
  }

  /**
    * Returns the difference between the two sets of KafkaClusterItems.
    *
    * @param items1 the first set of KafkaClusterItems
    * @param items2 the second set of KafkaClusterItems
    * @return the items missing from the first one, missing from the second one, and the ones that are different
    */
  def diff(
    items1: Seq[KafkaClusterItemOfTopology],
    items2: Seq[KafkaClusterItemOfTopology]
  ): DiffResult = {

    val items1Bykey = items1.map(i => (i.kafkaClusterItem.key, i)).toMap
    val items2Bykey = items2.map(i => (i.kafkaClusterItem.key, i)).toMap

    val keys1 =  items1Bykey.keySet
    val keys2 =  items2Bykey.keySet

    val missingFrom1 = (keys2 -- keys1).toIndexedSeq.map(items2Bykey(_))
    val missingFrom2 = (keys1 -- keys2).toIndexedSeq.map(items1Bykey(_))
    val changes = (keys1 intersect keys2).toIndexedSeq
      .map(k => (items1Bykey(k), items2Bykey(k)))
      .filter { case (item1, item2) => item1.kafkaClusterItem != item2.kafkaClusterItem }

    DiffResult(missingFrom1, missingFrom2, changes)
  }
}
