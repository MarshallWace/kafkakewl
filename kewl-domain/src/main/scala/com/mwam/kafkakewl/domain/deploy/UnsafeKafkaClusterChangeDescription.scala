/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

/**
  * Describes a property in an UnsafeKafkaClusterChangeDescription that's unsafe to change.
  *
  * @param name the name of the property.
  * @param change the change description as a human readable string (doesn't need to follow any structure, it's for humans)
  */
final case class UnsafeKafkaClusterChangePropertyDescription(name: String, change: String)

/**
  * The possible unsafe operations.
  */
sealed trait UnsafeKafkaClusterChangeOperation
object UnsafeKafkaClusterChangeOperation {
  final case object Remove extends UnsafeKafkaClusterChangeOperation
  final case object Update extends UnsafeKafkaClusterChangeOperation
}

/**
  * Describes a change that's considered unsafe (may cause data-loss or may break existing users of the kafka-cluster)
  *
  * This should be readable for humans and also possible to create regular expressions to allow certain (or all) unsafe-changes.
  *
  * @param operation the operation itself
  * @param entityType the type of the entity that's being changed unsafely.
  * @param entityKey the key of the entity that's being changed unsafely.
  * @param entityProperties the list of property descriptors of the entity that are being changed unsafely.
  */
final case class UnsafeKafkaClusterChangeDescription(
  operation: UnsafeKafkaClusterChangeOperation,
  entityType: KafkaClusterItemEntityType,
  entityKey: String,
  entityProperties: Seq[UnsafeKafkaClusterChangePropertyDescription] = Seq.empty
) {

  /**
    * Returns true if this unsafe-change is allowed based on the set of allowed unsafe changes.
    *
    * @param allowedUnsafeChanges the allowed unsafe changes
    * @return true if this unsafe-change is allowed based on the set of allowed unsafe changes
    */
  def isAllowed(allowedUnsafeChanges: Seq[DeploymentAllowUnsafeKafkaClusterChange]): Boolean = {
    operation match {
      case UnsafeKafkaClusterChangeOperation.Remove =>
        allowedUnsafeChanges.exists(a =>
          a.operation.forall(_ == operation) &&
          a.entityType.forall(_ == entityType) &&
          a.entityKey.forall(_ == entityKey) &&
          // removes are allowed if there is one allowed-change with entityPropertyName = None and the others matching the current change
          a.entityPropertyName.isEmpty
        )
      case UnsafeKafkaClusterChangeOperation.Update =>
        // for updates, all updated entity properties must be allowed
        entityProperties.forall(ep =>
          allowedUnsafeChanges.exists(a =>
            a.operation.forall(_ == operation) &&
            a.entityType.forall(_ == entityType) &&
            a.entityKey.forall(_ == entityKey) &&
            a.entityPropertyName.forall(_ == ep.name)
          )
        )
    }
  }
}
