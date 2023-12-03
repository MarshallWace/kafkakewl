/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.deploy.kafka

import org.apache.kafka.common.resource.PatternType
import zio.kafka.admin.acl.{AclOperation, AclPermissionType}
import zio.kafka.admin.resource.ResourceType

/** Base trait of all kafka cluster items (which are really just topics and acls).
  */
sealed trait KafkaClusterItem {
  val key: String
  val isReal = true
}

object KafkaClusterItem {

  /** A kafka-cluster item to represent a kafka topic.
    *
    * @param isReal
    *   true if it's an actual topic that should exist in the cluster or false if it's an un-managed topic which may or may not exist
    */
  final case class Topic(
      name: String,
      partitions: Int = 1,
      replicationFactor: Short = 3,
      config: Map[String, String],
      override val isReal: Boolean = true
  ) extends KafkaClusterItem {
    val key: String = s"topic:$name"
  }

  /** A kafka cluster item to represent a kafka acl.
    *
    * @note
    *   For now we use the zio.kafka types here, but if needed we can switch back to the vanilla kafka ones.
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

    /** The key of the acl is itself with all its fields. It's not possible to "update" and acl, only add or remove.
      */
    val key: String = s"acl:$resourceType/$resourcePatternType/$resourceName/$principal/$host/$operation/$permission"
  }
}
