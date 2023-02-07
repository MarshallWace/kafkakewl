/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import scala.reflect.runtime.universe._

/**
  * A trait for the logic that can compact entities.
  */
trait EntityCompactor[-E <: Entity] {
  type Compacted <: Entity
  def compact(e: E): Compacted
  // we'll need the TypeTag of the compacted type later, when it's only an abstract type,
  // but that gives some weird TypeTag, not the actual one
  // this way every compactor can return the right TypeTag and we can pass it on as an "explicit" implicit
  def typeTag: TypeTag[Compacted]
}

object EntityCompactor {
  /**
    * Returns a compactor that doesn't compact anything, just returns the input entity.
    *
    * @tparam E the entity type
    * @return the input entity
    */
  def noCompact[E <: Entity : TypeTag]: EntityCompactor[E] { type Compacted = E } = new EntityCompactor[E] {
    type Compacted = E
    def compact(e: E): Compacted = e
    def typeTag: TypeTag[Compacted] = implicitly[TypeTag[E]]
  }

  /**
    * Creates a compactor for the specified entity type and compacted entity type with the given function.
    *
    * @param compactFunc the function that does the compaction
    * @tparam E the entity type
    * @tparam C the compacted entity type
    * @return the compactor
    */
  def create[E <: Entity, C <: Entity : TypeTag](compactFunc: E => C): EntityCompactor[E] { type Compacted = C } = new EntityCompactor[E] {
    type Compacted = C
    def compact(e: E): Compacted = compactFunc(e)
    def typeTag: TypeTag[Compacted] = implicitly[TypeTag[C]]
  }
}

/**
  * The default compactors for all entities
  */
object EntityCompactors {
  implicit val permissionCompactor: EntityCompactor[permission.Permission] = EntityCompactor.noCompact
  implicit val kafkaClusterCompactor: EntityCompactor[kafkacluster.KafkaCluster] = EntityCompactor.noCompact
  implicit val topologyCompactor: EntityCompactor[topology.Topology] = EntityCompactor.create(topology.Topology.compact)
  implicit val deploymentCompactor: EntityCompactor[deploy.Deployment] = EntityCompactor.noCompact
  implicit val deployedTopologyCompactor: EntityCompactor[deploy.DeployedTopology] = EntityCompactor.create(deploy.DeployedTopology.compact)
}