/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.extensions.builtin

import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.headers.HttpChallenge
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive, Directive1}
import com.mwam.kafkakewl.extensions.AuthPlugin
import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

/**
  * Authentication plugin using kerberos authentication (com.tresata.akka.http.spnego.SpnegoDirectives.spnegoAuthenticate).
  */
class KerberosAuthPlugin extends AuthPlugin with LazyLogging {
  override def createAuthenticationDirective(config: Config): Directive1[String] = {
    val pluginConfig = config.getConfig("kafkakewl-extensions-builtin.kerberos-auth-plugin")
    val lowerCasePrincipal = pluginConfig.getBooleanOrNone("lowercase-principal").getOrElse(true)
    val removeSubString = pluginConfig.getStringOrNoneIfEmpty("remove-sub-string")

    logger.info(s"KerberosAuthPlugin lowercase-principal: $lowerCasePrincipal")
    logger.info(s"KerberosAuthPlugin remove-sub-string:   $removeSubString")

    com.tresata.akka.http.spnego.SpnegoDirectives.spnegoAuthenticate().map { token =>
      val principal = if (lowerCasePrincipal) token.principal.toLowerCase else token.principal
      removeSubString match {
        case Some(rss) => principal.replace(rss, "")
        case None => principal
      }
    }
  }
}

/**
  * A dummy authentication plugin always returning the same user-name.
  */
class SameUserAuthPlugin extends AuthPlugin with LazyLogging {
  override def createAuthenticationDirective(config: Config): Directive1[String] = {
    val pluginConfig = config.getConfig("kafkakewl-extensions-builtin.same-user-auth-plugin")
    val userName = pluginConfig.getStringOrNoneIfEmpty("user-name").getOrElse("unknown")

    logger.info(s"SameUserAuthPlugin user-name: $userName")
    // we just call the inner with provided username
    Directive[Tuple1[String]](inner =>
      ctx => inner(Tuple1(userName))(ctx)
    )
  }
}

/**
  * A simple authentication plugin returning a particular header's value as the user-name.
  */
class HttpHeaderAuthPlugin extends AuthPlugin with LazyLogging {
  override def createAuthenticationDirective(config: Config): Directive1[String] = {
    val pluginConfig = config.getConfig("kafkakewl-extensions-builtin.http-header-auth-plugin")
    val headerName: String = pluginConfig.getStringOrNoneIfEmpty("header-name")
      .getOrElse(sys.error(s"$name expects the header-name config value to be populated with an http header name"))
    val authenticationRejectedScheme: String = pluginConfig.getStringOrNoneIfEmpty("rejected-scheme").getOrElse(headerName)
    val authenticationRejectedRealm: String = pluginConfig.getStringOrNoneIfEmpty("rejected-realm").getOrElse("")

    logger.info(s"HttpHeaderAuthPlugin header-name: $headerName")

    def extractHeader: HttpHeader => Option[String] = {
      case HttpHeader(`headerName`, value) => Some(value)
      case _ => None
    }

    optionalHeaderValue(extractHeader).flatMap {
      // we got a value for this header, send it on as the user-name
      case Some(userName) => Directive[Tuple1[String]](inner => ctx => inner(Tuple1(userName))(ctx))
      // not value for this header, rejecting with authentication failed
      case None => reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, HttpChallenge(authenticationRejectedScheme, authenticationRejectedRealm)))
    }
  }
}
