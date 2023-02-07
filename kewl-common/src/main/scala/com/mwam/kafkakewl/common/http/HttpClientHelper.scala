/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import akka.actor.ActorSystem
import akka.http.scaladsl.coding.{Deflate, Gzip, NoCoding}
import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.model.headers.HttpEncodings
import akka.stream.Materializer
import cats.syntax.either._
import io.circe.Decoder
import io.circe.parser.decode
import com.mwam.kafkakewl.domain.{CommandError, ValueOrCommandError}

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration._

object HttpClientHelper {
  final case class StringResponse(response: HttpResponse, contentString: String)
}

trait HttpClientHelper {
  def decodeResponse(response: HttpResponse): HttpResponse = {
    val decoder = response.encoding match {
      case HttpEncodings.gzip => Gzip
      case HttpEncodings.deflate => Deflate
      case _ => NoCoding
    }
    decoder.decodeMessage(response)
  }

  def toStringResponse(
    responseFuture: Future[HttpResponse]
  )(implicit system: ActorSystem, ec: ExecutionContextExecutor, mat: Materializer): Future[HttpClientHelper.StringResponse] = {
    for {
      response <- responseFuture.map(decodeResponse)
      responseString <- response.entity.toStrict(10.seconds).map(_.data.utf8String)
    } yield HttpClientHelper.StringResponse(response, responseString)
  }

  def decodeResponse[T : Decoder](stringResponse :HttpClientHelper.StringResponse): ValueOrCommandError[T] = {
    stringResponse.response.status match {
      case StatusCodes.OK => decode[T](stringResponse.contentString).leftMap(CommandError.exception)
      case _ => CommandError.otherError(stringResponse.contentString).asLeft
    }
  }
}
