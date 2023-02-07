/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.routes

import akka.actor.ActorRef
import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.pattern.ask
import akka.http.scaladsl.server.{Directive1, StandardRoute}
import akka.http.scaladsl.server.directives.ParameterDirectives._
import akka.http.scaladsl.server.directives.RouteDirectives
import akka.util.Timeout
import com.mwam.kafkakewl.domain.{Command, CommandError, CommandErrorType, CommandResult, CommandResultBase, KafkaClusterCommandResult}
import com.mwam.kafkakewl.utils._
import com.mwam.kafkakewl.common.http.HttpExtensions._
import com.mwam.kafkakewl.domain.JsonEncodeDecode._
import com.mwam.kafkakewl.processor.state.StateReadOnlyCommandProcessor
import nl.grons.metrics4.scala.{DefaultInstrumented, MetricName}

import scala.concurrent.{ExecutionContextExecutor, Future}

final case class MetricNames(firstMetricName: String, moreMetricNames: String*) {
  def metricNames: Seq[String] = Seq(firstMetricName) ++ moreMetricNames
}

trait RouteUtils extends DefaultInstrumented {
  override lazy val metricBaseName = MetricName("com.mwam.kafkakewl.api.route")

  def populateMetrics(beforeNanoTime: Long, statusCode: StatusCode)(implicit metricNames: MetricNames): Unit ={
    val duration = durationSince(beforeNanoTime)
    metricNames.metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:all").update(duration))
    metricNames.metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:all").mark())
    if (statusCode == StatusCodes.OK) {
      metricNames.metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:success").update(duration))
      metricNames.metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:success").mark())
    } else {
      metricNames.metricNames.foreach(metricName => metrics.timer(s"$metricName:duration:failure").update(duration))
      metricNames.metricNames.foreach(metricName => metrics.meter(s"$metricName:meter:failure").mark())
    }
  }

  def completeFuture(fre: Future[_])(implicit ec: ExecutionContextExecutor, metricNames: MetricNames): StandardRoute = {
    val beforeNanoTime = System.nanoTime()
    RouteDirectives.complete(
      fre
        .map { _ =>
          val statusCode = StatusCodes.OK
          populateMetrics(beforeNanoTime, statusCode)
          responseAsTextWithStatusCode("OK", statusCode)
        }
        .recover { case _ =>
          val statusCode = StatusCodes.InternalServerError
          populateMetrics(beforeNanoTime, statusCode)
          responseAsTextWithStatusCode("Failed", statusCode)
        }
    )
  }

  def completeCommand(
    fre: Future[CommandResult]
  )(implicit ec: ExecutionContextExecutor, metricNames: MetricNames): StandardRoute = {
    def hasAnyErrorsOfType(failed: CommandResultBase.Failed, errorType: CommandErrorType) = failed.reasons.exists(_.errorType == errorType)
    def hasAnyPermissionErrors = hasAnyErrorsOfType(_: CommandResultBase.Failed, CommandErrorType.Permission)
    def hasAnyValidationErrors = hasAnyErrorsOfType(_: CommandResultBase.Failed, CommandErrorType.Validation)
    def hasAnyExceptionsOrOther(failed: CommandResultBase.Failed) = failed.reasons.exists(e => e.errorType == CommandErrorType.Exception || e.errorType == CommandErrorType.Other)
    def hasAnyError(failed: CommandResultBase.Failed) = true

    def hasKafkaClusterFailure(errorPredicate: CommandResultBase.Failed => Boolean)(result: KafkaClusterCommandResult) = result match {
      case f: KafkaClusterCommandResult.Failed => errorPredicate(f)
      case _ => false
    }

    val beforeNanoTime = System.nanoTime()
    RouteDirectives.complete(
      fre.map {
        case s: CommandResult.Succeeded =>
          val statusCode = {
            if (s.kafkaClusterResults.exists(hasKafkaClusterFailure(hasAnyExceptionsOrOther))) StatusCodes.InternalServerError
            else if (s.kafkaClusterResults.exists(hasKafkaClusterFailure(hasAnyValidationErrors))) StatusCodes.BadRequest
            else if (s.kafkaClusterResults.exists(hasKafkaClusterFailure(hasAnyError))) StatusCodes.InternalServerError
            else StatusCodes.OK
          }
          populateMetrics(beforeNanoTime, statusCode)
          responseAsJsonWithStatusCode(s: CommandResult, statusCode)

        case f: CommandResult.Failed =>
          val statusCode =
            if (hasAnyExceptionsOrOther(f)) StatusCodes.InternalServerError
            else if (hasAnyPermissionErrors(f)) StatusCodes.Unauthorized
            else if (hasAnyValidationErrors(f)) StatusCodes.BadRequest
            else StatusCodes.InternalServerError
          populateMetrics(beforeNanoTime, statusCode)
          responseAsJsonWithStatusCode(f: CommandResult, statusCode)
      }
    )
  }

  implicit class ProcessorActorExtensions(processor: ActorRef)(implicit ec: ExecutionContextExecutor, timeout: Timeout) {
    def ??(command: Command)(implicit metricNames: MetricNames): StandardRoute = {
      completeCommand((processor ? command).mapTo[CommandResult])
    }
  }

  implicit class ReadOnlyProcessorActorExtensions(readOnlyProcessor: StateReadOnlyCommandProcessor)(implicit ec: ExecutionContextExecutor) {
    def ??(command: Command.StateReadOnlyCommand)(implicit metricNames: MetricNames): StandardRoute = {
      completeCommand(Future.successful(readOnlyProcessor.processCommand(command)))
    }
  }

  def dryRunParam: Directive1[Boolean] = parameters('dryRun ? "false", 'dryrun ? "false") // TODO this is ugly, I'm sure there is a better way of doing it
    .tmap { case (dryRunString, dryrunString) =>
      // making sure we accept either dryRun or dryrun, and if any of them is not false, we consider it a dry-run, to be on the safe-side
      val dryRun = dryRunString.toLowerCase.toBooleanOrNone.getOrElse(true)
      val dryrun = dryrunString.toLowerCase.toBooleanOrNone.getOrElse(true)
      dryRun || dryrun
    }

  def compactParam: Directive1[Boolean] = parameters('compact ? "false")
    .tmap { case Tuple1(compactString) => compactString.toLowerCase.toBooleanOrNone.getOrElse(true) }

  def failure(command: Command, commandError: CommandError)(implicit ec: ExecutionContextExecutor, metricNames: MetricNames): StandardRoute =
    completeCommand(Future.successful(command.failedResult(commandError)))
}
