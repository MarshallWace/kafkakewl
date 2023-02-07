/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

import cats.syntax.option._
import com.mwam.kafkakewl.domain.deploy.{UnsafeKafkaClusterChangeDescription, UnsafeKafkaClusterChangeOperation, UnsafeKafkaClusterChangePropertyDescription}
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigKeys
import com.mwam.kafkakewl.domain.kafkacluster.IsTopicConfigManaged
import com.mwam.kafkakewl.utils._

/**
  * A base trait for all the changes that can happen to the kafka cluster.
  */
private[kafkacluster] sealed trait KafkaClusterChange {
  /**
    * The key of the KafkaClusterItem that's being changed.
    */
  def key: String

  def isReal: Boolean

  /**
    * Returns an optional UnsafeKafkaClusterChangeDescription instance that describes the current change's unsafe part.
    *
    * Unsafe changes are all the removals (topic or acl) and certain topic changes (e.g. reducing retention.ms or turning on log compaction, etc..)
    * These changes require explicit approval from the user. Without that we execute only the safe part of the change.
    *
    * By default it returns None which means there is no unsafe part of this change (the current change is completely safe).
    */
  def unsafeChange: Option[UnsafeKafkaClusterChangeDescription] = None

  /**
    * Create a safe change of the current one (in case the unsafe is not allowed). None means there is no safe change and current change is completely unsafe.
    *
    * By default it returns the current change which means, that this change is safe.
    */
  def makeSafe: Option[KafkaClusterChange] = Some(this)
  def toTuple: (String, KafkaClusterChange) = (key, this)
}
private[kafkacluster] object KafkaClusterChange {
  private val defaultTopicRetentionMs = 7 * 24 * 3600 * 1000L
  private val defaultTopicCleanupPolicy = "delete"

  /**
    * Represents adding a new KafkaClusterItem (topic or ACL) to the cluster.
    */
  final case class Add(item: KafkaClusterItem) extends KafkaClusterChange {
    def key: String = item.key
    def isReal: Boolean = item.isReal
    override def toString: String = s"add: [$item]"
  }

  /**
    * Represents a removal a KafkaClusterItem (topic or ACL).
    */
  final case class Remove(item: KafkaClusterItem) extends KafkaClusterChange {
    def key: String = item.key
    def isReal: Boolean = item.isReal
    override def unsafeChange: Option[UnsafeKafkaClusterChangeDescription] =
      Some(UnsafeKafkaClusterChangeDescription(UnsafeKafkaClusterChangeOperation.Remove, item.entityType, item.key))
    override def makeSafe: Option[KafkaClusterChange] = None
    override def toString: String = s"delete: [$item]"
  }

  /**
    * Represents an kafka topic update change.
    */
  final case class UpdateTopic(beforeItem: KafkaClusterItem.Topic, afterItem: KafkaClusterItem.Topic) extends KafkaClusterChange {
    assert(beforeItem.key == afterItem.key)

    private def makePartitionChangeSafe(targetTopic: KafkaClusterItem.Topic): (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic) = {
      if (beforeItem.partitions < targetTopic.partitions)
        (Some(UnsafeKafkaClusterChangePropertyDescription("partitions", s"${beforeItem.partitions}->${targetTopic.partitions}")), targetTopic.copy(partitions = beforeItem.partitions))
      else (None, targetTopic)
    }

    private def makeReplicationFactorChangeSafe(targetTopic: KafkaClusterItem.Topic): (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic) = {
      (beforeItem.replicationFactor, targetTopic.replicationFactor) match {
        case (beforeReplicationFactor, afterReplicationFactor) if beforeReplicationFactor == afterReplicationFactor =>
          (None, targetTopic)
        case (beforeReplicationFactor, -1) if beforeReplicationFactor > 0 =>
          // changing the topic to replicationFactor = -1 comes with replica-placements too which is just a config change => considered safe
          (None, targetTopic)
        case (-1, afterReplicationFactor) if afterReplicationFactor > 0 =>
          // changing to a positive replicationFactor from -1 (i.e. replica-placements) is unsafe, and we can make it safe by keeping replicationFactor = -1 and adding the replica-placement topic config
          (
            Some(UnsafeKafkaClusterChangePropertyDescription("replicationFactor", s"${beforeItem.replicationFactor}->${targetTopic.replicationFactor}")),
            targetTopic.copy(
              replicationFactor = -1,
              // adding back the replicaPlacement of the before-topic (otherwise replicationFactor = -1 is invalid)
              config = beforeItem.config.get(TopicConfigKeys.confluentPlacementConstraints) match {
                case Some(replicaPlacement) => targetTopic.config + (TopicConfigKeys.confluentPlacementConstraints -> replicaPlacement)
                case None => targetTopic.config // this can't really happen: if the replicationFactor is -1 of an actual kafka topic, that must mean that there is replicaPlacement, nevertheless we just use the original config
              }
            )
          )
        case _ =>
          // both are positive and different
          (
            Some(UnsafeKafkaClusterChangePropertyDescription("replicationFactor", s"${beforeItem.replicationFactor}->${targetTopic.replicationFactor}")),
            targetTopic.copy(replicationFactor = beforeItem.replicationFactor)
          )
      }
    }

    private def makeConfigChangeSafe(
      configKey: String,
      isConfigValueChangeUnsafe: (Option[String], Option[String]) => Boolean,
      afterItem: KafkaClusterItem.Topic
    ): (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic) = {
      val beforeConfigValueOrNone = beforeItem.config.get(configKey)
      val afterConfigValueOrNone = afterItem.config.get(configKey)
      if (isConfigValueChangeUnsafe(beforeConfigValueOrNone, afterConfigValueOrNone)) {
        val beforeConfigValue = beforeConfigValueOrNone.getOrElse("")
        val afterConfigValue = afterConfigValueOrNone.getOrElse("")
        val safeAfterItemConfig =
          if (beforeItem.config.contains(configKey))
            afterItem.config + (configKey -> beforeItem.config(configKey))
          else
            afterItem.config - configKey
        (Some(UnsafeKafkaClusterChangePropertyDescription(configKey, s"$beforeConfigValue->$afterConfigValue")), afterItem.copy(config = safeAfterItemConfig))
      } else (None, afterItem)
    }


    private def makeConfigRetentionMsChangeSafe(targetTopic: KafkaClusterItem.Topic): (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic) = {
      makeConfigChangeSafe(
        "retention.ms",
        (before, after) => before.flatMap(_.toLongOrNone).getOrElse(defaultTopicRetentionMs) > after.flatMap(_.toLongOrNone).getOrElse(defaultTopicRetentionMs),
        targetTopic
      )
    }

    private def makeConfigLogCompactionChangeSafe(targetTopic: KafkaClusterItem.Topic): (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic) = {
      makeConfigChangeSafe(
        "cleanup.policy",
        (before, after) => before.getOrElse(defaultTopicCleanupPolicy) == "compact" && after.getOrElse(defaultTopicCleanupPolicy) == "delete",
        targetTopic
      )
    }

    // invoking all make-safe functions in a chain
    private val makeSafeFunctions = List[KafkaClusterItem.Topic => (Option[UnsafeKafkaClusterChangePropertyDescription], KafkaClusterItem.Topic)](
      makePartitionChangeSafe,
      makeReplicationFactorChangeSafe,
      makeConfigRetentionMsChangeSafe,
      makeConfigLogCompactionChangeSafe)

    // gathering all unsafe changes and making the target topic to be safe
    private val (unsafeChangeProperties, safeTargetTopic) = makeSafeFunctions.foldLeft(
      (IndexedSeq.empty[UnsafeKafkaClusterChangePropertyDescription], afterItem))((result, makeSafeFunc) => {
      val (unsafeChanges, targetTopic) = result
      val (unsafeChangeOrNone, newTargetTopic) = makeSafeFunc(targetTopic)
      (unsafeChanges ++ unsafeChangeOrNone.toIndexedSeq, newTargetTopic)
    })

    def key: String = beforeItem.key
    def isReal: Boolean = afterItem.isReal

    override def unsafeChange: Option[UnsafeKafkaClusterChangeDescription] =
      if (unsafeChangeProperties.isEmpty) {
        None
      } else {
        Some(UnsafeKafkaClusterChangeDescription(UnsafeKafkaClusterChangeOperation.Update, afterItem.entityType, beforeItem.key, unsafeChangeProperties))
      }

    override def makeSafe: Option[KafkaClusterChange] =
      if (unsafeChangeProperties.isEmpty) {
        None
      } else {
        Some(UpdateTopic(beforeItem, safeTargetTopic))
      }

    /**
     * Creates a new topic update change or none after ignoring the topic configs that are not managed by kafkakewl.
     *
     * If a config key is not managed by kafkakewl, it still may exists in the actual kafka topic (e.g. confluent platform uses it for internal tools).
     * In this case kafkakewl must ignore the difference between the topology's topic and the kafka topic and pretend they are equal.
     *
     * This is done by adding the not-managed config values from the kafka topic (beforeItem) into the topology's topic's (afterItem) config.
     *
     * @param isTopicConfigManaged a function deciding whether a config key is managed by kafkakewl or not
     */
    def ignoreNotManagedTopicConfigs(isTopicConfigManaged: IsTopicConfigManaged): Option[KafkaClusterChange] = {
      val beforeItemNotManagedKeys = beforeItem.config.keys.filter(key => !isTopicConfigManaged(key)).toSet

      // this is the config that needs to be added into afterItem from beforeItem (so that these configs are equal and kafkakewl won't try to remove the ones in before but not in after)
      val configFromBeforeIntoAfter = beforeItemNotManagedKeys
        .flatMap { beforeItemNotAllowedKey =>
          afterItem.config.get(beforeItemNotAllowedKey) match {
            case Some(_) =>
              // the after-item also has this config, leave that as it is - it'll be a possible difference, but that's fine
              None
            case None =>
              // the after-item doesn't have this config, so we ensure that it'll leave the before's value there
              (beforeItemNotAllowedKey, beforeItem.config(beforeItemNotAllowedKey)).some
          }
      }.toMap

      val newAfterItem = afterItem.copy(
        config = afterItem.config ++ configFromBeforeIntoAfter,
        // special rule for replication-factor: if we're adding the "confluent.placement.constraints" config from beforeItem into afterItem,
        // we need to change the replicationFactor too so that it matches beforeItem's (which must be -1)
        replicationFactor = {
          if (configFromBeforeIntoAfter.contains(TopicConfigKeys.confluentPlacementConstraints)) {
            // this really must be -1
            beforeItem.replicationFactor
          } else {
            // just leave it as it is
            afterItem.replicationFactor
          }
        }
      )

      val newChange = UpdateTopic(beforeItem, newAfterItem)
      if (configFromBeforeIntoAfter.nonEmpty) {
        // only killing the equal before-after if we did any config change in afterItem (it's really just being careful, wanted to keep the current behavior if we don't mess with the afterItem's config)
        if (beforeItem != newAfterItem) {
          newChange.some
        } else {
          // it's possible that now the beforeItem and newAfterItem are equal, so we return a none change
          None
        }
      } else {
        newChange.some
      }
    }

    override def toString: String = s"update: [$beforeItem] -> [$afterItem]"
  }
}
