/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpOrigin, HttpOriginRange}
import akka.http.scaladsl.server.Directives.{complete, delete, get, headerValue, options, post, provide, put, reject, respondWithHeaders}
import akka.http.scaladsl.server.{Directive, Directive0, Route}
import akka.http.scaladsl.server.Directives._

trait CORSHandler{
  val allAllowedOrigins: Seq[HttpOrigin]
  val additionalAllowedHeaders: Seq[String] = Seq.empty

  private def corsResponseHeaders(origins: Seq[HttpOrigin]) = List(
      headers.`Access-Control-Allow-Origin`.forRange(HttpOriginRange(origins: _*)),
      headers.`Access-Control-Allow-Credentials`(true),
      headers.`Access-Control-Allow-Headers`(Seq("Authorization", "Content-Type", "X-Requested-With", "X-Remote-User", "Accept") ++ additionalAllowedHeaders: _*)
    )

  private def extractOrigin: HttpHeader => Option[Seq[HttpOrigin]] = {
    case h: headers.`Origin` => Some(h.origins)
    case _ => None
  }

  private def withAccessControlHeaders: Directive0 =
    (headerValue(extractOrigin) | provide(Seq.empty[HttpOrigin])) .flatMap { origins =>
      // always accept anything from localhost
      val allowedOrigins = origins.filter(o => o.host.host.address.equalsIgnoreCase("localhost") || allAllowedOrigins.contains(o))
      if (allowedOrigins.nonEmpty) {
        // send back CORS headers only for the matching origins
        respondWithHeaders(corsResponseHeaders(allowedOrigins))
      } else {
        // if there is no origin at all, just do nothing
        Directive.Empty
      }
    }

  private def preflightRequestHandler: Route =
    options {
      complete(HttpResponse(StatusCodes.OK).
        withHeaders(headers.`Access-Control-Allow-Methods`(HttpMethods.OPTIONS, HttpMethods.POST, HttpMethods.PUT, HttpMethods.GET, HttpMethods.DELETE)))
    } ~
    // so that for get/post/put/delete we cancel the previous method-rejection (if it's not option)
    // and the rest of the route will not be impacted (especially the spnego directive which also operates with a special rejection to do the negotiation)
    // Don't know if it's the best solution, but it works...
    get { reject() } ~
    post { reject() } ~
    put { reject() } ~
    delete { reject() }

  def corsHandler(r: Route): Route = withAccessControlHeaders { preflightRequestHandler ~ r }
}
