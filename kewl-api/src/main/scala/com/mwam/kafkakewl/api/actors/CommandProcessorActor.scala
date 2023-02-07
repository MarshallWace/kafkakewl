/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.actors

import akka.actor.Actor
import akka.pattern.{ask, pipe}
import cats.data.EitherT
import cats.instances.future._
import com.mwam.kafkakewl.common._
import com.mwam.kafkakewl.common.persistence.PersistentStoreFactory
import com.mwam.kafkakewl.common.validation.Validation._
import com.mwam.kafkakewl.common.validation.{PermissionStoreBuiltin, PermissionValidator}
import com.mwam.kafkakewl.domain.Command._
import com.mwam.kafkakewl.domain.EntityCompactors._
import com.mwam.kafkakewl.domain._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology
import com.mwam.kafkakewl.domain.kafkacluster.{KafkaCluster, KafkaClusterEntityId}
import com.mwam.kafkakewl.domain.permission.Permission
import com.mwam.kafkakewl.domain.topology.TopologyLike.TopicDefaults
import com.mwam.kafkakewl.processor.kafkacluster.KafkaClusterCommandProcessorActor
import com.mwam.kafkakewl.processor.state.StateCommandProcessor
import com.mwam.kafkakewl.utils.{ApplicationMetrics, MdcUtils}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object CommandProcessorActor {
  final case class KafkaClusterCommandProcessorConfig(
    env: Env,
    jaasConfig: Option[String],
    failFastIfDeploymentStateStoreInvalid: Boolean
  )
}

class CommandProcessorActor(
  val persistentStoreFactory: PersistentStoreFactory,
  val kafkaClusterProcessorConfig: CommandProcessorActor.KafkaClusterCommandProcessorConfig,
  val permissionValidator: PermissionValidator,
  val permissionStoreBuiltinExtensionOrNone: Option[PermissionStoreBuiltin],
  createStateCommandProcessor: Boolean => StateCommandProcessor,
  val topicDefaults: TopicDefaults
) extends Actor
  // not using akka's ActorLogging because that's async and everywhere else we use the normal LazyLogging (performance won't matter anyway)
  with ActorPreRestartLog
  with CommandProcessorExtensions
  with KafkaClusterProcessorParentActor
  with PermissionCommandResultCommon
  with MdcUtils {

  import context.dispatcher

  private var stateCommandProcessor: StateCommandProcessor = _

  // overriden members
  val logger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
  def kafkaClusterStateStore: ReadableStateStore[KafkaCluster] = stateStores.kafkaCluster
  def stateStores: AllStateEntities.ReadableVersionedStateStores = stateCommandProcessor.stateStores
  def validateCommandPermissions(command: Command): Result = permissionValidator.validateCommandPermissions(stateStores, command)
  def validateCommandPermissions(command: KafkaClusterCommand): Result = permissionValidator.validateCommandPermissions(stateStores, command)
  def updateDeployedTopologyStateStores(deployedTopologyStateStores: Map[KafkaClusterEntityId, ReadableStateStore[DeployedTopology]]): Unit =
    stateCommandProcessor.updateDeployedTopologyStateStores(deployedTopologyStateStores)

  def logCommandResult(command: CommandBase, cr: CommandResult): CommandResult = {
    withMDC(command.mdc) {
      logger.info(s"response: ${cr.truncatedToString}")
      cr
    }
  }

  implicit class ResultFutureExt(crf: Future[CommandResult]) {
    def log(command: CommandBase): Future[CommandResult] = crf.map(logCommandResult(command, _))
  }

  // lazy because the stateCommandProcessor can be initialized later and that holds the state-stores
  private lazy val applyPermissionsTo = applyPermissionsToCommandResult(permissionValidator, stateStores) _

  override def preStart(): Unit = {
    logger.info(s"preStart() starting...")

    super.preStart()

    stateCommandProcessor = createStateCommandProcessor(false)
    initialiseKafkaClusterActors(kafkaClusterStateStore.getLatestLiveStates)

    logger.info(s"preStart() finished.")
  }

  override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    msg match {
      case commandBase: CommandBase =>
        withMDC(commandBase.mdc) {
          super.aroundReceive(receive, msg)
        }
      case _ =>
        super.aroundReceive(receive, msg)
    }
  }

  override def receive: Receive = {
    case command: HealthIsLive =>
      logger.info(s"received: $command")
      // it's live when it received this request
      // NOT asking all kafka-cluster processors whether they are live too
      // because a slow or struggling cluster can fail the liveness/readiness checks and kill kafkakewl (with prod-critical monitoring)
      // It's not ideal, but this is the best we can do right now.
//        this.kafkaClusterProcessorActors
//          .map { case (kafkaClusterId, kafkaClusterProcessActor) =>
//            (kafkaClusterProcessActor ? command)
//              .mapTo[HealthIsLiveResponse]
//              .recover { case t => HealthIsLiveResponse(t) }
//              .map(_.withErrorPrefix(s"$kafkaClusterId: "))
//          }
//      ).map { responses => responses.foldLeft(HealthIsLiveResponse(live = true, Seq.empty))(_ + _) } pipeTo sender()
      sender() ! HealthIsLiveResponse(live = true, Seq.empty)

    case command: HealthIsReady =>
      logger.info(s"received: $command")
      // it's ready when it received this request (preStart sorted out all start-up operations)
      // NOT asking all kafka-cluster processors whether they are ready too
      // because a slow or struggling cluster can fail the liveness/readiness checks and kill kafkakewl (with prod-critical monitoring)
      // It's not ideal, but this is the best we can do right now.
//      Future.sequence(
//        this.kafkaClusterProcessorActors
//          .map { case (kafkaClusterId, kafkaClusterProcessActor) =>
//            (kafkaClusterProcessActor ? command)
//              .mapTo[HealthIsReadyResponse]
//              .recover { case t => HealthIsReadyResponse(t) }
//              .map(_.withErrorPrefix(s"$kafkaClusterId: "))
//          }
//      ).map { responses => responses.foldLeft(HealthIsReadyResponse(ready = true, Seq.empty))(_ + _) } pipeTo sender()
      sender() ! HealthIsReadyResponse(ready = true, Seq.empty)

    case command: PermissionPluginGetUserPermissions =>
      logger.info(s"received: $command")
      val result = for {
        _ <- permissionValidator.validateCommandPermissions(stateStores, command).toResultOrError(command)
      } yield {
        permissionStoreBuiltinExtensionOrNone
          .map { permissionStoreBuiltinExtension =>
            val permissions = permissionStoreBuiltinExtension.getPermissions(command.userNameOfPermissions)
            command.succeededResult(
              permissions
                .zipWithIndex
                // these are not real entities, so the id, version and createdBy are fake
                .map { case (p, index) => EntityState.Live[Permission](EntityStateMetadata(s"#$index", 1, "unknown"), p) }
                .toIndexedSeq,
              compact = false
            )
          }
          .getOrElse { command.failedResult(CommandError.otherError("there is no external permission plugin configured")) }
      }
      sender() ! logCommandResult(command, result.merge)

    case command: PermissionPluginInvalidateCachedUserPermissions =>
      logger.info(s"received: $command")
      val result = for {
        _ <- permissionValidator.validateCommandPermissions(stateStores, command).toResultOrError(command)
      } yield {
        permissionStoreBuiltinExtensionOrNone
          .map { permissionStoreBuiltinExtension =>
            permissionStoreBuiltinExtension.invalidateCachedPermissions(command.userNameOfPermissions)
            command.succeededResult()
          }
          .getOrElse { command.failedResult(CommandError.otherError("there is no external permission plugin configured")) }
      }
      sender() ! logCommandResult(command, result.merge)

    case command: ReInitialize =>
      logger.warn(s"received: $command")
      val result = for {
        _ <- permissionValidator.validateCommandPermissions(stateStores, command).toResultOrError(command)
      } yield {
        // need to save the current kafka-clusters so that we can reinitialize with diffing it to the new ones
        val beforeKafkaClusterStateStore = kafkaClusterStateStore
        // re-create the state processor (if needed, wipe it)
        stateCommandProcessor = createStateCommandProcessor(command.wipe)
        // and re-initialize the kafka-cluster processor actors (wipe if needed, kill or restart)
        reInitialiseKafkaClusterActors(beforeKafkaClusterStateStore, kafkaClusterStateStore.getLatestLiveStates, command.wipe)

        command.succeededResult()
      }
      sender() ! logCommandResult(command, result.merge)

    case command: KafkaDiff =>
      logger.info(s"received: $command")
      processToKafkaClusterCommand(command).value.toCommandResult.log(command) pipeTo sender()

    case kafkaClusterMessage: KafkaClusterCommandProcessorActor.OutgoingMessage =>
      handleKafkaClusterProcessorMessage(kafkaClusterMessage)

    case command: Command with Command.ToKafkaClusterCommand with Command.KafkaClusterIdCommand =>
      logger.info(s"received: $command")
      val resultF = for {
        unfilteredResult <- processToKafkaClusterCommand(command)
        filteredResult <- EitherT.fromEither(applyPermissionsTo(command, unfilteredResult))
      } yield filteredResult
      resultF.value.toCommandResult.log(command) pipeTo sender()

    case command: Command.StateCommand =>
      logger.info(s"received: $command")
      val resultF = for {
        // EitherT[Future, CommandResult.Failed, CommandResult.Succeeded]
        // needs to use futures because the communication to the kafka-cluster processor actors is async

        // executing the command
        stateCommandResult <- EitherT.fromEither[Future](stateCommandProcessor.processCommand(command))

        // handle kafka-cluster changes
        _ = handleKafkaClusterStateChanges(stateCommandResult.changes.kafkaCluster)

        // permission checking on the generated kafka-cluster commands
        _ <- EitherT.fromEither[Future](
          stateCommandResult.kafkaClusterCommands
            .map(permissionValidator.validateCommandPermissions(stateStores, _))
            .combine().toResultOrError(command))

        // executing the possible kafka-cluster commands and gathering all results in a single future
        kafkaClusterCommandResults <- {
          val kafkaClusterCommandsAndProcessors = stateCommandResult.kafkaClusterCommands.map(kcc => (kcc, kafkaClusterProcessorActors.get(kcc.kafkaClusterId)))
          kafkaClusterCommandsAndProcessors
            .foreach {
              case (kcc, None) =>
                logger.error(s"this shouldn't happen: can't dispatch kafka cluster command $kcc, because we don't have a processing actor for ${kcc.kafkaClusterId}")
                ApplicationMetrics.errorCounter.inc()
              case _ => ()
            }
          val kafkaClusterCommandFutures = kafkaClusterCommandsAndProcessors
            .collect {case (kcc, Some(processor)) => (kcc, (processor ? kcc).mapTo[KafkaClusterCommandProcessorActor.Result]) }

          EitherT.right[CommandResult.Failed](Future.sequence(kafkaClusterCommandFutures.map(_._2)))
        }
      } yield stateCommandResult.commandResult.copy(kafkaClusterResults =  kafkaClusterCommandResults.map(_.commandResult))
      resultF.value.toCommandResult.log(command) pipeTo sender()

    case msg =>
      ApplicationMetrics.errorCounter.inc()
      logger.error(s"received unknown message: $msg")
  }

  override def postStop(): Unit = {
    logger.info(s"postStop() starting...")
    super.postStop()
    stateCommandProcessor.close()
    logger.info(s"postStop() finished.")
  }
}
