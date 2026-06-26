/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.mwam.kafkakewl.utils.ApplicationMetrics
import com.typesafe.scalalogging.LazyLogging

object ReadOnlyHttpAuthorizer {
  // the HTTP methods that only read state (or are CORS pre-flight) and are therefore always allowed
  val readOnlyMethods: Set[String] = Set("GET", "HEAD", "OPTIONS")
}

/**
 * A coarse, configurable authorization rule that restricts users whose name matches a regex to read-only HTTP access:
 * any method other than [[ReadOnlyHttpAuthorizer.readOnlyMethods]] (GET/HEAD/OPTIONS) is rejected with 403 Forbidden.
 *
 * It is not a replacement for the fine-grained permission system; it is an additional coarse gate applied before the
 * routes, e.g. to stop a class of users from performing any mutating request.
 *
 * The user regex uses the same partial-match semantics (findFirstIn) as the disallowed topology name regexes.
 *
 * @param readOnlyUserNameRegex users whose name matches this regex are restricted to read-only access; if None the rule is disabled.
 */
class ReadOnlyHttpAuthorizer(readOnlyUserNameRegex: Option[String]) extends LazyLogging {
  import ReadOnlyHttpAuthorizer._

  private val readOnlyUserPattern = readOnlyUserNameRegex.map(_.r)

  /**
   * The pure authorization decision: is this user rejected for this HTTP method?
   *
   * As a deny-rule it errs on the side of rejecting more, never less, so a substring match cannot widen into an
   * authorization bypass.
   */
  def isRejected(user: String, method: String): Boolean =
    !readOnlyMethods.contains(method.toUpperCase) &&
      readOnlyUserPattern.exists(_.findFirstIn(user).isDefined)

  /**
   * Wraps the given route, rejecting the request with 403 Forbidden if the authenticated user is restricted to
   * read-only access and the request uses a non-read-only method, otherwise delegating to the wrapped route.
   */
  def authorize(user: String)(inner: => Route): Route =
    extractMethod { method =>
      if (isRejected(user, method.value)) {
        ApplicationMetrics.authorizationRejectedCounter.inc()
        extractUri { uri =>
          logger.error(s"read-only authorizer REJECTED user '$user' performing ${method.value} request to $uri")
          complete(HttpResponse(
            StatusCodes.Forbidden,
            entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"user '$user' is restricted to read-only (GET/HEAD/OPTIONS) access")
          ))
        }
      } else {
        inner
      }
    }
}
