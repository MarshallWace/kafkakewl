/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import java.util.UUID

import cats.Monoid
import com.mwam.kafkakewl.domain.deploy.DeploymentStateChange
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterStateChange
import com.mwam.kafkakewl.domain.permission.PermissionStateChange
import com.mwam.kafkakewl.domain.topology.TopologyStateChange

final case class AllStateEntitiesStateChanges(
  permission: IndexedSeq[PermissionStateChange.StateChange] = IndexedSeq.empty,
  topology: IndexedSeq[TopologyStateChange.StateChange] = IndexedSeq.empty,
  kafkaCluster: IndexedSeq[KafkaClusterStateChange.StateChange] = IndexedSeq.empty,
  deployment: IndexedSeq[DeploymentStateChange.StateChange] = IndexedSeq.empty
)

object AllStateEntitiesStateChanges {
  def apply(c: PermissionStateChange.StateChange): AllStateEntitiesStateChanges = apply(permission = IndexedSeq(c))
  def apply(c: TopologyStateChange.StateChange): AllStateEntitiesStateChanges = apply(topology = IndexedSeq(c))
  def apply(c: KafkaClusterStateChange.StateChange): AllStateEntitiesStateChanges = apply(kafkaCluster = IndexedSeq(c))
  def apply(c: DeploymentStateChange.StateChange): AllStateEntitiesStateChanges = apply(deployment = IndexedSeq(c))

  implicit class AllStateEntitiesStateChangesExtensions(changes: AllStateEntitiesStateChanges) {
    // TODO: do not forget to add new entities here (missing it from here doesn't mean compile error)
    def stateChanges: IndexedSeq[IndexedSeq[_]] = IndexedSeq(changes.permission, changes.topology, changes.kafkaCluster, changes.deployment)
    def size: Int = stateChanges.map(_.size).sum
    def isEmpty: Boolean = size == 0
    def nonEmpty: Boolean = !isEmpty

    def toTransactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllStateEntitiesStateChanges]] = {
      val transactionId = UUID.randomUUID().toString.replace("-", "")
      val transactionItems = IndexedSeq(
        // TODO: do not forget to add new entities here (missing it from here doesn't mean compile error)
        changes.permission.map(AllStateEntitiesStateChanges(_)),
        changes.topology.map(AllStateEntitiesStateChanges(_)),
        changes.kafkaCluster.map(AllStateEntitiesStateChanges(_)),
        changes.deployment.map(AllStateEntitiesStateChanges(_))
      ).flatten

      transactionItems
        .zipWithIndex
        // the last item in the transaction must be marked as "last", so that consumers know when the transaction ended
        .map { case (change, index) => EntityStateChangeTransactionItem(transactionId, index == transactionItems.length - 1, change)}
    }
  }

  implicit val allStateEntitiesMonoid: Monoid[AllStateEntitiesStateChanges] = new Monoid[AllStateEntitiesStateChanges] {
    override def empty: AllStateEntitiesStateChanges = AllStateEntitiesStateChanges()
    override def combine(x: AllStateEntitiesStateChanges, y: AllStateEntitiesStateChanges): AllStateEntitiesStateChanges =
      AllStateEntitiesStateChanges(
        x.permission ++ y.permission,
        x.topology ++ y.topology,
        x.kafkaCluster ++ y.kafkaCluster,
        x.deployment ++ y.deployment
      )
  }
}
