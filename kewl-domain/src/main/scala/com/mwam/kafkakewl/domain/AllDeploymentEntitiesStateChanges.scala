/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import java.util.UUID

import cats.Monoid
import com.mwam.kafkakewl.domain.deploy.DeployedTopologyStateChange

/**
  * Entity state changes
  */
final case class AllDeploymentEntitiesStateChanges(
  deployedTopology: IndexedSeq[DeployedTopologyStateChange.StateChange] = IndexedSeq.empty
) {
  def isEmpty: Boolean = deployedTopology.isEmpty
  def nonEmpty = !isEmpty
}

object AllDeploymentEntitiesStateChanges {
  def apply(c: DeployedTopologyStateChange.StateChange): AllDeploymentEntitiesStateChanges = apply(deployedTopology = IndexedSeq(c))

  implicit class AllDeploymentEntitiesStateChangesExtensions(changes: AllDeploymentEntitiesStateChanges) {
    // TODO: do not forget to add new entities here (missing it from here doesn't mean compile error)
    def stateChanges: IndexedSeq[IndexedSeq[_]] = IndexedSeq(changes.deployedTopology)
    def size: Int = stateChanges.map(_.size).sum
    def isEmpty: Boolean = size == 0
    def nonEmpty: Boolean = !isEmpty

    def toTransactionItems: IndexedSeq[EntityStateChangeTransactionItem[AllDeploymentEntitiesStateChanges]] = {
      val transactionId = UUID.randomUUID().toString.replace("-", "")
      val transactionItems = IndexedSeq(
        // TODO: do not forget to add new entities here (missing it from here doesn't mean compile error)
        changes.deployedTopology.map(AllDeploymentEntitiesStateChanges(_))
      ).flatten

      transactionItems
        .zipWithIndex
        // the last item in the transaction must be marked as "last", so that consumers know when the transaction ended
        .map { case (change, index) => EntityStateChangeTransactionItem(transactionId, index == transactionItems.length - 1, change)}
    }
  }

  implicit val allDeploymentEntitiesMonoid: Monoid[AllDeploymentEntitiesStateChanges] = new Monoid[AllDeploymentEntitiesStateChanges] {
    override def empty: AllDeploymentEntitiesStateChanges = AllDeploymentEntitiesStateChanges()
    override def combine(x: AllDeploymentEntitiesStateChanges, y: AllDeploymentEntitiesStateChanges): AllDeploymentEntitiesStateChanges =
      AllDeploymentEntitiesStateChanges(
        x.deployedTopology ++ y.deployedTopology,
      )
  }
}
