/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.StatusCodes.InternalServerError
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import com.mwam.kafkakewl.domain.CommandResult
import com.mwam.kafkakewl.utils._
import com.typesafe.scalalogging.Logger
import io.circe.Encoder
import io.circe.syntax._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

object HttpExtensions {
  def notFoundResponse(entityName: Option[String] = None) : ToResponseMarshallable = {
    HttpResponse(
      StatusCodes.NotFound,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, entityName.map(e => s"$e not found").getOrElse(s"not found"))
    )
  }

  def internalServerErrorResponse(e: String) : ToResponseMarshallable = {
    HttpResponse(
      StatusCodes.InternalServerError,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, e)
    )
  }

  def internalServerErrorResponse(e: Throwable) : ToResponseMarshallable = {
    HttpResponse(
      StatusCodes.InternalServerError,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, e.toErrorMessage)
    )
  }

  def responseAsJsonWithStatusCode[T](r: T, status: StatusCode = StatusCodes.OK)(implicit en: Encoder[T]) : ToResponseMarshallable = {
    HttpResponse(
      status,
      entity = HttpEntity(ContentTypes.`application/json`, r.asJson.noSpaces)
    )
  }

  def responseAsTextWithStatusCode(r: String, status: StatusCode = StatusCodes.OK): ToResponseMarshallable = {
    HttpResponse(
      status,
      entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, r)
    )
  }

  implicit class OptionHttpExtensions[T](o: Option[T]) {
    def toRightOrNotFound(entityName: Option[String] = None): Either[ToResponseMarshallable, T] = {
      o match {
        case Some(v) => Right(v)
        case None => Left(notFoundResponse(entityName))
      }
    }
  }

  implicit class TryHttpExtensions[T](t: Try[T]) {
    def toRightOrError: Either[ToResponseMarshallable, T] = {
      t match {
        case Success(r) => Right(r)
        case Failure(e) => Left(internalServerErrorResponse(e))
      }
    }
  }

  implicit class CommandResultFutureExtensions(fre: Future[CommandResult]) {
  }

  def defaultExceptionHandler(logger: Logger): ExceptionHandler = ExceptionHandler {
    case t =>
      ApplicationMetrics.errorCounter.inc()
      extractUri { uri =>
        logger.error(s"Unhandled exception while serving $uri", t)
        complete(
          HttpResponse(
            InternalServerError,
            entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, t.getMessage)
          )
        )
      }
  }

  /**
   * Creates a route "GET test/exception/{optional-message}" that just throws an exception.
   *
   * It should be used only as a test route.
   */
  def testExceptionRoute(implicit ec: ExecutionContextExecutor): Route = {
    ignoreTrailingSlash {
      pathPrefix("test" / "exception") {
        get {
          path(Segment) { errorMessage =>
            sys.error(errorMessage)
          } ~
            pathEnd {
              sys.error("test exception")
            }
        }
      }
    }
  }
}
