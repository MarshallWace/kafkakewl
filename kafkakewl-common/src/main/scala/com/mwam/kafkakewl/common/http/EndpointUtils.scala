/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import sttp.model.StatusCode
import sttp.tapir.*
import sttp.tapir.CodecFormat.TextPlain
import sttp.tapir.EndpointIO.annotations.*
import zio.json.{JsonDecoder, JsonEncoder}

trait EndpointUtils {
  val apiEndpoint: PublicEndpoint[Unit, Unit, Unit, Any] =
    endpoint.in("api" / "v1")
}

object EndpointUtils {

  /** Body codec that will deserialize YAML into domain objects via YAML -> JSON
    * -> domain marshalling. This codec does not support encoding responses as
    * YAML (since circe-yaml does not support encoding) and YAML response are
    * not expected to see use.
    *
    * The Content-Type of YAML requests is associated with "text/plain".
    *
    * This works by using circe-yaml to convert the YAML input from the request
    * body into JSON and using the existing tapir JSON marshalling onwards.
    *
    * @tparam T
    *   Domain object target of the model
    * @return
    *   A body mapping from string to T
    */
  def yamlRequestBody[T: JsonEncoder: JsonDecoder: Schema]
      : EndpointIO.Body[String, T] = {
    val jsonDecode = sttp.tapir.json.zio.zioCodec[T]
    stringBodyUtf8AnyFormat(
      zioYamlCodec.mapDecode(jsonDecode.rawDecode)(jsonDecode.encode)
    )
  }

  private val zioYamlCodec: Codec[String, String, TextPlain] =
    sttp.tapir.Codec.anyString[String, TextPlain](CodecFormat.TextPlain()) {
      s =>
        io.circe.yaml.parser.parse(s).map(_.spaces2) match {
          case Left(error)       => DecodeResult.Error(s, error)
          case Right(jsonString) => DecodeResult.Value(jsonString)
        }
    } { _ =>
      throw UnsupportedOperationException("Encoding as YAML is unsupported")
    }
}

case class ErrorResponse(
    @jsonbody message: String,
    @statusCode statusCode: StatusCode
)
