/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import java.time.{Clock, OffsetDateTime}

import scala.reflect.runtime.universe._

import com.mwam.kafkakewl.utils._

/**
  * Base trait for all entity id types.
  */
trait EntityId extends Any {
  def id: String
  override def toString: String = id
  def isEmpty: Boolean = id.trim.isEmpty
  def quote: String = id.quote
}

/**
  * A base trait for all entity types (e.g. Topology, KafkaCluster, Permission, etc...)
  */
trait Entity {
  // nothing for now, it's only a grouping-trait
}

object Entity {
  implicit class EntityTypeExtensions[E <: Entity](t: Type) {
    def entityName: String = t.typeSymbol.name.toString.toLowerCase
  }

  implicit class EntityExtensions[E <: Entity](entity: E) {
    def toLiveState(metadata: EntityStateMetadata): EntityState.Live[E] = EntityState.Live[E](metadata, entity)
  }
}

/**
  * A few common metadata-like fields for entity states
  *
  * @param id the id of the entity
  * @param createdBy the user name of the user who created this state
  * @param version the version of the entity
  * @param timeStampUtc the creation utc time-stamp of the entity state
  */
final case class EntityStateMetadata(
  id: String,
  version: Int,
  createdBy: String,
  timeStampUtc: OffsetDateTime = OffsetDateTime.now(Clock.systemUTC()),
)

trait EntityStateMetadataOwner {
  val metadata: EntityStateMetadata

  def id: String = metadata.id
  def version: Int = metadata.version
  def createdBy: String = metadata.createdBy
  def timeStampUtc: OffsetDateTime = metadata.timeStampUtc
}

/**
  * Represents the state of the entity. Can be either a valid live state or indicating that the entity is deleted.
  *
  * @tparam E the type of the entity
  */
sealed trait EntityState[+E <: Entity] extends EntityStateMetadataOwner {
  def liveOrNone: Option[EntityState.Live[E]]
}
object EntityState {
  /**
    * The state of a valid, live entity.
    *
    * @param metadata the metadata of the entity state
    * @param entity the entity instance itself
    * @tparam E the type of the entity
    */
  final case class Live[+E <: Entity](
    metadata: EntityStateMetadata,
    entity: E
  ) extends EntityState[E] {
    def liveOrNone: Option[Live[E]] = Some(this)
  }

  /**
    * The state of a deleted entity.
    *
    * @param metadata the metadata of the entity state
    * @tparam E the type of the entity
    */
  final case class Deleted[+E <: Entity](
    metadata: EntityStateMetadata
  ) extends EntityState[E] {
    def liveOrNone: Option[EntityState.Live[E]] = None
  }
}

/**
  * Labels and tags.
  */
trait Labelled {
  val tags: Seq[String]
  val labels: Map[String, String]
}
