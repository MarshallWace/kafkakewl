/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package permission

import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto._

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._

  implicit val permissionEntityIdEncoder: Encoder[PermissionEntityId] = Encoder[String].contramap(_.id)
  implicit val permissionEntityIdDecoder: Decoder[PermissionEntityId] = Decoder[String].map(PermissionEntityId)
  implicit val permissionResourceTypeEncoder: Encoder[PermissionResourceType] = deriveEnumerationEncoder
  implicit val permissionResourceTypeDecoder: Decoder[PermissionResourceType] = deriveEnumerationDecoder
  implicit val permissionResourceOperationEncoder: Encoder[PermissionResourceOperation] = deriveEnumerationEncoder
  implicit val permissionResourceOperationDecoder: Decoder[PermissionResourceOperation] = deriveEnumerationDecoder

  implicit val permissionEncoder: Encoder[Permission] = deriveConfiguredEncoder
  implicit val permissionDecoder: Decoder[Permission] = deriveConfiguredDecoder

  implicit val permissionEntityStateChangeEncoder: Encoder[PermissionStateChange.StateChange] = deriveConfiguredEncoder
  implicit val permissionEntityStateChangeDecoder: Decoder[PermissionStateChange.StateChange] = deriveConfiguredDecoder
}
