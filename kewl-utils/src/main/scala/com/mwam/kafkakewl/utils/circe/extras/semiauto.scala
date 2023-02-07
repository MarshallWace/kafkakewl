/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils.circe.extras

import io.circe.generic.extras.Configuration
import io.circe.generic.extras.decoding.ConfiguredDecoder
import io.circe.{Decoder, DecodingFailure, HCursor}
import shapeless.ops.hlist
import shapeless.ops.record.Keys
import shapeless.{HList, LabelledGeneric, Lazy}
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import shapeless.LabelledGeneric.Aux

object semiauto {
  def makeDecoderStrict[T, Repr <: HList, KeysRepr <: HList](decoder: Decoder[T])(
    implicit
    lgen: LabelledGeneric.Aux[T, Repr],
    keys: Keys.Aux[Repr, KeysRepr],
    toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
    configuration: Configuration,
    decoderEvidence: Lazy[ConfiguredDecoder[T]]
  ): Decoder[T] = {
    lazy val expectedFields = Keys[Repr]
      .apply()
      .toList
      .map(fieldSymbol => configuration.transformMemberNames(fieldSymbol.name))
      .toSet

    c: HCursor => {
      decoder
        .apply(c)
        .flatMap(o => {
          val unexpectedFieldsOrNone = for {
            json <- c.focus
            jsonKeys <- json.hcursor.keys
          } yield jsonKeys.filterNot(expectedFields)

          if (unexpectedFieldsOrNone.exists(_.isEmpty)) {
            // no unexpected fields
            Right(o)
          } else {
            unexpectedFieldsOrNone
              .map(uf => Left(DecodingFailure(s"unexpected json fields of ${o.getClass.getSimpleName}: ${uf.map(ufi => s"'$ufi'").mkString(", ")}", List.empty)))
              .getOrElse(Left(DecodingFailure(s"couldn't check unexpected json fields", List.empty)))

          }
        })
    }
  }

  implicit class DecoderExtensions[T](decoder: Decoder[T]) {
    def makeStrict[Repr <: HList, KeysRepr <: HList](
      implicit
      lgen: LabelledGeneric.Aux[T, Repr],
      keys: Keys.Aux[Repr, KeysRepr],
      toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
      configuration: Configuration,
      decoderEvidence: Lazy[ConfiguredDecoder[T]]
    ): Decoder[T] = makeDecoderStrict(decoder)

    def makeStrictFor[Repr <: HList, KeysRepr <: HList](lgen: LabelledGeneric.Aux[T, Repr])(
      implicit
      keys: Keys.Aux[Repr, KeysRepr],
      toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
      configuration: Configuration,
      decoderEvidence: Lazy[ConfiguredDecoder[T]]
    ): Decoder[T] = makeDecoderStrictFor(decoder, lgen)
  }

  def makeDecoderStrictFor[T, Repr <: HList, KeysRepr <: HList](decoder: Decoder[T], lgen: LabelledGeneric.Aux[T, Repr])(
    implicit
    keys: Keys.Aux[Repr, KeysRepr],
    toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
    configuration: Configuration,
    decoderEvidence: Lazy[ConfiguredDecoder[T]]
  ): Decoder[T] = {
    implicit val lgenImpl: Aux[T, Repr] = lgen
    makeDecoderStrict(decoder)
  }

  def deriveStrictDecoder[T, Repr <: HList, KeysRepr <: HList](
    implicit
    lgen: LabelledGeneric.Aux[T, Repr],
    keys: Keys.Aux[Repr, KeysRepr],
    toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
    configuration: Configuration,
    decoderEvidence: Lazy[ConfiguredDecoder[T]]
  ): Decoder[T] = makeDecoderStrict(deriveConfiguredDecoder[T])

  def deriveStrictDecoderFor[T, Repr <: HList, KeysRepr <: HList](lgen: LabelledGeneric.Aux[T, Repr])(
    implicit
    keys: Keys.Aux[Repr, KeysRepr],
    toTraversable: hlist.ToTraversable.Aux[KeysRepr, List, Symbol],
    configuration: Configuration,
    decoderEvidence: Lazy[ConfiguredDecoder[T]]
  ): Decoder[T] = makeDecoderStrictFor(deriveConfiguredDecoder[T], lgen)
}
