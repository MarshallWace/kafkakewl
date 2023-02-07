/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.actors

import akka.actor.{Actor, ActorRef, PoisonPill, Props}
import akka.pattern.ask
import akka.util.Timeout
import cats.data.EitherT
import cats.instances.future._
import com.mwam.kafkakewl.kafka.utils.{KafkaConnection, KafkaConnectionInfo}
import com.mwam.kafkakewl.common.persistence.PersistentStoreFactory
import com.mwam.kafkakewl.common.validation.Validation
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.domain.deploy.{DeployedTopology, DeployedTopologyStateChange}
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.domain.{Command, CommandError, CommandResponse, CommandResult, EntityState, KafkaClusterCommand, KafkaClusterCommandResult}
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessorActor
import com.mwam.kafkakewl.utils.ApplicationMetrics
import com.typesafe.scalalogging.Logger

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * A trait that's used by an actor that wants to be a parent of the kafka-cluster processor actors.
  *
  * It manages the kafka-cluster processor actors, receives their deployed topologies.
  */
trait KafkaClusterProcessorParentActor {
  this: Actor with CommandProcessorExtensions =>

  import context.dispatcher

  // abstract members
  val logger: Logger
  val topicDefaults: TopicDefaults
  val persistentStoreFactory: PersistentStoreFactory
  val kafkaClusterProcessorConfig: CommandProcessorActor.KafkaClusterCommandProcessorConfig
  def kafkaClusterStateStore: ReadableStateStore[KafkaCluster]
  def validateCommandPermissions(command: Command): Validation.Result
  def validateCommandPermissions(command: KafkaClusterCommand): Validation.Result
  def updateDeployedTopologyStateStores(deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]): Unit

  // concrete members
  implicit val timeout: Timeout = 60.seconds

  private val processorActors = mutable.Map[KafkaClusterEntityId, ActorRef]()
  private val kafkaClusterDeployedTopologies = mutable.Map[KafkaClusterEntityId, InMemoryStateStore[DeployedTopology, DeployedTopologyStateChange.StateChange]]()
  private val kafkaClusters = mutable.Map[KafkaClusterEntityId, KafkaCluster]()

  private def updateDeployedTopologyStateStoresCache(): Unit = {
    val stateStoresSnapshot = kafkaClusterDeployedTopologies.mapValues(ss => ss.readableSnapshot).toMap
    updateDeployedTopologyStateStores(stateStoresSnapshot)
  }

  private def removeKafkaClusterProcessorActor(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster): Unit = {
    logger.info(s"removing kafka cluster processor $kafkaClusterId = [$kafkaCluster.toShortString]")
    processorActors(kafkaClusterId) ! PoisonPill
    processorActors.remove(kafkaClusterId)
    kafkaClusterDeployedTopologies.remove(kafkaClusterId)
    kafkaClusters -= kafkaClusterId

    updateDeployedTopologyStateStoresCache()
  }

  private def addKafkaClusterProcessorActor(kafkaClusterId: KafkaClusterEntityId, kafkaCluster: KafkaCluster): Unit = {
    logger.info(s"adding kafka cluster processor $kafkaClusterId = [${kafkaCluster.toShortString}]")
    val kafkaConnection = KafkaConnection(KafkaConnectionInfo(kafkaCluster.brokers, kafkaCluster.securityProtocol, kafkaCluster.kafkaClientConfig), kafkaClusterProcessorConfig.jaasConfig)
    val kafkaClusterProcessorActor = context.actorOf(
      Props(
        new KafkaClusterCommandProcessorActor(
          KafkaClusterCommandProcessorActor.Config(
            kafkaClusterId,
            kafkaClusterProcessorConfig.env,
            kafkaConnection,
            kafkaCluster,
            topicDefaults,
            kafkaClusterProcessorConfig.failFastIfDeploymentStateStoreInvalid
          ),
          persistentStoreFactory
        )
      ),
      s"KafkaClusterCommandProcessorActor-$kafkaClusterId"
    )
    processorActors.put(kafkaClusterId, kafkaClusterProcessorActor)
    kafkaClusters += (kafkaClusterId -> kafkaCluster)
  }

  private def updateKafkaClusterProcessorActor(kafkaClusterId: KafkaClusterEntityId, beforeKafkaCluster: KafkaCluster, afterKafkaCluster: KafkaCluster): Unit = {
    logger.info(s"updating kafka cluster processor $kafkaClusterId = [$beforeKafkaCluster.toShortString] -> [$afterKafkaCluster.toShortString]")
    processorActors(kafkaClusterId) ! KafkaClusterCommandProcessorActor.UpdateKafkaCluster(afterKafkaCluster)
    kafkaClusters(kafkaClusterId) = afterKafkaCluster
  }

  def handleKafkaClusterStateChanges(kafkaClusterStateChangeResults: IndexedSeq[ApplyStateChange.ApplyResult[KafkaCluster]]): Unit = {
    if (kafkaClusterStateChangeResults.nonEmpty) {
      logger.info(s"received kafka cluster changes $kafkaClusterStateChangeResults from the state processor.")
    }
    kafkaClusterStateChangeResults
      .flatMap(_.state)
      .foreach { state =>
        val currentKafkaClusterState = kafkaClusters.get(KafkaClusterEntityId(state.id))
        (currentKafkaClusterState, state) match {
          case (Some(bs), v: EntityState.Live[KafkaCluster]) => updateKafkaClusterProcessorActor(KafkaClusterEntityId(v.id), bs, v.entity)
          case (None, v: EntityState.Live[KafkaCluster]) => addKafkaClusterProcessorActor(KafkaClusterEntityId(v.id), v.entity)
          case (Some(bs), d: EntityState.Deleted[KafkaCluster]) => removeKafkaClusterProcessorActor(KafkaClusterEntityId(d.id), bs)
          case (None, d: EntityState.Deleted[KafkaCluster]) =>
            logger.error(s"this shouldn't happen: kafka cluster ${d.id} doesn't exist, but it's being removed from the store (it means the local state-store and the state-processor's are out of sync)")
            ApplicationMetrics.errorCounter.inc()
        }
      }
  }

  def initialiseKafkaClusterActors(
    kafkaClusters: Seq[EntityState.Live[KafkaCluster]]
  ): Unit = kafkaClusters.foreach(s => addKafkaClusterProcessorActor(KafkaClusterEntityId(s.id), s.entity))

  def reInitialiseKafkaClusterActors(
    beforeKafkaClusterStateStore: ReadableStateStore[KafkaCluster],
    kafkaClusters: Seq[EntityState.Live[KafkaCluster]],
    wipe: Boolean
  ): Unit = {
    // diff-ing the new set of clusters with the old one
    val before = beforeKafkaClusterStateStore.getLatestLiveStates.map(s => (KafkaClusterEntityId(s.id), s.entity)).toMap
    val after = kafkaClusters.map(s => (KafkaClusterEntityId(s.id), s.entity)).toMap
    val beforeKeys = before.keySet
    val afterKeys = after.keySet
    val toAdd = (afterKeys -- beforeKeys).map(k => (k, after(k)))
    val toRemove = (beforeKeys -- afterKeys).map(k => (k, before(k)))
    val toUpdate = (beforeKeys intersect afterKeys).map(k => (k, before(k), after(k)))
    toAdd.foreach { case (k, kc) => addKafkaClusterProcessorActor(k, kc) }
    toRemove.foreach { case (k, kc) =>
      processorActors(k) ! KafkaClusterCommandProcessorActor.ReInitialize(wipe)
      removeKafkaClusterProcessorActor(k, kc)
    }
    // TODO if we allow changing brokers/security protocol I need to close and re-create the kafka cluster processor actor (be careful with re-creation, need to wait until the previous actor properly terminates)
    toUpdate.foreach { case (k, bkc, akc) =>
      processorActors(k) ! KafkaClusterCommandProcessorActor.ReInitialize(wipe)
      // if they are equal, no point sending an update
      if (bkc != akc) updateKafkaClusterProcessorActor(k, bkc, akc)
    }
  }

  def kafkaClusterProcessorActors: Map[KafkaClusterEntityId, ActorRef] = processorActors.toMap

  def handleKafkaClusterProcessorMessage(message: KafkaClusterCommandProcessorActor.OutgoingMessage): Unit =
    message match {
      case KafkaClusterCommandProcessorActor.DeployedTopologies(kafkaClusterId, deployedTopologies) =>
        logger.info(s"received ${deployedTopologies.size} deployed topologies from the kafka cluster processor $kafkaClusterId.")
        processorActors.get(kafkaClusterId) match {
          case Some(_) => kafkaClusterDeployedTopologies(kafkaClusterId) = InMemoryStateStore.forDeployedTopology(deployedTopologies)
          case None =>
            logger.error(s"kafka cluster $kafkaClusterId doesn't exist, maybe it's already being destroyed.")
            ApplicationMetrics.errorCounter.inc()
        }
        updateDeployedTopologyStateStoresCache()

      case KafkaClusterCommandProcessorActor.DeployedTopologyChanges(kafkaClusterId, deployedTopologyChanges) =>
        logger.info(s"received deployed topology changes $deployedTopologyChanges from the kafka cluster processor $kafkaClusterId.")
        kafkaClusterDeployedTopologies.get(kafkaClusterId) match {
          case Some(kafkaClusterDeployedTopology) => deployedTopologyChanges.foreach(kafkaClusterDeployedTopology.applyEntityStateChange)
          case None =>
            logger.error(s"this shouldn't happen: kafka cluster $kafkaClusterId doesn't exist.")
            ApplicationMetrics.errorCounter.inc()
        }
        updateDeployedTopologyStateStoresCache()

      case KafkaClusterCommandProcessorActor.GetKafkaCluster(kafkaClusterId) =>
        logger.info(s"received GetKafkaCluster($kafkaClusterId).")
        kafkaClusterStateStore.getLatestLiveState(kafkaClusterId) match {
          case Some(s) => sender() ! KafkaClusterCommandProcessorActor.UpdateKafkaCluster(s.entity)
          case None =>
            logger.error(s"kafka cluster $kafkaClusterId doesn't exist, maybe the kafka cluster processing actor is about to be destroyed anyway?")
            ApplicationMetrics.errorCounter.inc()
        }
    }

  def processToKafkaClusterCommand(
    command: Command with Command.ToKafkaClusterCommand with Command.KafkaClusterIdCommand
  ): EitherT[Future, CommandResult.Failed, CommandResult.Succeeded] = {
    for {
      // EitherT[Future, CommandResult.Failed, CommandResult.Succeeded]
      _ <- EitherT.fromEither[Future](validateCommandPermissions(command).toResultOrError(command))

      // finding the kafka cluster command processor actor
      kafkaClusterProcessorActor <- EitherT.fromEither[Future](
        kafkaClusterProcessorActors.get(command.kafkaClusterId)
          .toRight(command.failedResult(CommandError.otherError(s"This shouldn't happen: couldn't find a kafka cluster processor actor for '${command.kafkaClusterId}'"))))

      // finally let the kafka cluster command processor actor to execute the command
      kafkaClusterCommand = command.toKafkaClusterCommand

      // checking permissions for the kafka-cluster command
      _ <- EitherT.fromEither[Future](validateCommandPermissions(kafkaClusterCommand).toResultOrError(command))

      kafkaClusterCommandResult <- EitherT.right[CommandResult.Failed]((kafkaClusterProcessorActor ? kafkaClusterCommand).mapTo[KafkaClusterCommandProcessorActor.Result])

      // converting the kafka-cluster result to a normal one (still may fail here)
      succeededResult <- kafkaClusterCommandResult.commandResult match {
        case s: KafkaClusterCommandResult.Succeeded =>
          EitherT.rightT[Future, CommandResult.Failed](command.succeededResult(CommandResponse.FromKafkaCluster(s.response)))
        case f: KafkaClusterCommandResult.Failed =>
          EitherT.leftT[Future, CommandResult.Succeeded](command.failedResult(f.reasons))
      }
    } yield succeededResult
  }
}
