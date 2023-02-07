/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.state

import cats.Monoid
import com.mwam.kafkakewl.domain.{AllStateEntitiesStateChanges, CommandResult, KafkaClusterCommand}
import com.mwam.kafkakewl.domain.AllStateEntitiesStateChanges._

private[state] object StateCommandProcessingResult {
  /**
    * The entity changes and kafka cluster commands as a result of a command processing.
    *
    * @param entityChanges the entity state changes as a result of the processing
    * @param kafkaClusterCommands the kafka cluster commands as a result of the processing
    */
  final case class EntityChangesWithKafkaClusterCommands(
    entityChanges: AllStateEntitiesStateChanges = AllStateEntitiesStateChanges(),
    kafkaClusterCommands: Seq[KafkaClusterCommand] = Seq.empty
  ) {
    def ++(other: EntityChangesWithKafkaClusterCommands)(implicit m: Monoid[AllStateEntitiesStateChanges]): EntityChangesWithKafkaClusterCommands = {
      EntityChangesWithKafkaClusterCommands(
        m.combine(entityChanges, other.entityChanges),
        kafkaClusterCommands ++ other.kafkaClusterCommands
      )
    }
  }

  /**
    * The result (success) of the command processing.
    *
    * @param commandResult the command processing result or None if there isn't any
    * @param changesAndCommands the entity state changes and kafka-cluster commands.
    */
  final case class Result(
    commandResult: CommandResult.Succeeded,
    changesAndCommands: EntityChangesWithKafkaClusterCommands = EntityChangesWithKafkaClusterCommands()
  ) {
    def ++(otherChangesAndCommands: EntityChangesWithKafkaClusterCommands): Result =
      Result(commandResult, changesAndCommands ++ otherChangesAndCommands)
  }

  /**
    * Fail-fast result for the command processing functions. It's either a CommandResult.Failed or the result above.
    */
  type ProcessFuncResult = Either[CommandResult.Failed, Result]
  object ProcessFuncResult {
    def success(result: Result): ProcessFuncResult = Right(result)
    def success(
      commandResult: CommandResult.Succeeded,
      entityChanges: AllStateEntitiesStateChanges,
      kafkaClusterCommands: IndexedSeq[KafkaClusterCommand]
    ): ProcessFuncResult = success(Result(commandResult, EntityChangesWithKafkaClusterCommands(entityChanges, kafkaClusterCommands)))
    def success(
      commandResult: CommandResult.Succeeded,
      entityChanges: AllStateEntitiesStateChanges
    ): ProcessFuncResult = success(commandResult, entityChanges, IndexedSeq.empty)
    def success(
      commandResult: CommandResult.Succeeded
    ): ProcessFuncResult = success(commandResult, AllStateEntitiesStateChanges())
    def failure(failed: CommandResult.Failed): ProcessFuncResult = Left(failed)
  }
}
