/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.deployment

private[kafkacluster] object KafkaClusterChanges {
  /**
    * Creates KafkaClusterChange.Remove changes from the specified KafkaClusterItems to be removed.
    *
    * @param itemsToRemove the KafkaClusterItems to be removed
    * @return the KafkaClusterChange.Remove changes
    */
  def remove(itemsToRemove: Iterable[KafkaClusterItem]): Iterable[KafkaClusterChange] = itemsToRemove.map(KafkaClusterChange.Remove)

  /**
    * Creates KafkaClusterChange.UpdateTopic changes from the specified KafkaClusterItem changes.
    *
    * @param updates the KafkaClusterItem changes, only KafkaClusterItem.Topic -> KafkaClusterItem.Topic is acceptable
    * @return the KafkaClusterChange.UpdateTopic changes
    */
  def update(updates: Iterable[(KafkaClusterItem, KafkaClusterItem)]): Iterable[KafkaClusterChange] =
    updates.map {
      case (before: KafkaClusterItem.Topic, after: KafkaClusterItem.Topic) => KafkaClusterChange.UpdateTopic(before, after)
      // only topics can change in a kafka cluster, nothing else (ACLs can't)
      case (before, after) => throw new RuntimeException(s"Invalid kafka cluster update: before = $before, after = $after")
    }

  /**
    * Creates KafkaClusterChange.Add changes from the specified KafkaClusterItems to be added.
    *
    * @param itemsToAdd the KafkaClusterItems to be added
    * @return the KafkaClusterChange.Add changes
    */
  def add(itemsToAdd: Iterable[KafkaClusterItem]): Iterable[KafkaClusterChange] = itemsToAdd.map(KafkaClusterChange.Add)

  /**
    * Merges the two optional KafkaClusterChanges (for the same key) into one, making sure that the result is equivalent with performing both
    * changes in this order.
    *
    * @param first the first change to perform
    * @param second the second change to perform.
    * @return a single optional KafkaClusterChange
    */
  def merge(first: Option[KafkaClusterChange], second: Option[KafkaClusterChange]): Option[KafkaClusterChange] = (first, second) match {
    case (Some(c1), None) => Some(c1)

    case (None, Some(c2)) => Some(c2)

    case (Some(KafkaClusterChange.Remove(remove1: KafkaClusterItem.Topic)), Some(KafkaClusterChange.Add(add2: KafkaClusterItem.Topic))) =>
      // removing a topic then adding it back can mean an update (if the new one is different from the old one)
      if (remove1 != add2) Some(KafkaClusterChange.UpdateTopic(remove1, add2))
      else None

    case (Some(KafkaClusterChange.Remove(_: KafkaClusterItem.Acl)), Some(KafkaClusterChange.Add(_: KafkaClusterItem.Acl))) =>
      // removing and ACL and adding it back means nothing to do, as there is no update for ACLs
      None

    case (Some(KafkaClusterChange.Add(_)), Some(KafkaClusterChange.Remove(_))) => None

    case (Some(KafkaClusterChange.UpdateTopic(before1, _)), Some(KafkaClusterChange.UpdateTopic(_, after2))) =>
      // two topic-updates result in one, from the first before to the second after state (if they are different at all)
      if (before1 != after2) Some(KafkaClusterChange.UpdateTopic(before1, after2))
      else None

    case (Some(KafkaClusterChange.Add(_: KafkaClusterItem.Topic)), Some(KafkaClusterChange.UpdateTopic(_, after2))) =>
      Some(KafkaClusterChange.Add(after2))

    case (Some(KafkaClusterChange.UpdateTopic(_, _)), Some(KafkaClusterChange.Remove(remove2: KafkaClusterItem.Topic))) =>
      // remove after update: skip the update, just remove
      Some(KafkaClusterChange.Remove(remove2))

    case (c1, c2) => throw new RuntimeException(s"Invalid deployed-to-before and before-to-after kafka-cluster changes $c1, $c2")
  }
}
