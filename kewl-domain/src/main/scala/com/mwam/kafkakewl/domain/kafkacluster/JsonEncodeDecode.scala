/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package kafkacluster

import cats.syntax.functor._
import com.mwam.kafkakewl.domain.kafka.config.TopicConfigDefault
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto._
import io.circe.syntax._
import io.circe.{Decoder, Encoder}

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._

  // entities
  implicit val kafkaClusterEncoder: Encoder[KafkaCluster] = encoderWithoutDefaultLabelled(deriveConfiguredEncoder)
  implicit val kafkaClusterDecoder: Decoder[KafkaCluster] = deriveConfiguredDecoder

  implicit val kafkaClusterEntityStateChangeEncoder: Encoder[KafkaClusterStateChange.StateChange] = deriveConfiguredEncoder
  implicit val kafkaClusterEntityStateChangeDecoder: Decoder[KafkaClusterStateChange.StateChange] = deriveConfiguredDecoder

  implicit val topicConfigDefaultEncoder: Encoder[TopicConfigDefault] = deriveConfiguredEncoder
  implicit val topicConfigDefaultDecoder: Decoder[TopicConfigDefault] = deriveConfiguredDecoder

  implicit val topicConfigKeyConstraintEncoder: Encoder[TopicConfigKeyConstraint] = Encoder.instance {
    // an exact flexible-name gets encoded as a simple string
    case n: TopicConfigKeyConstraint.Exact => n.exact.asJson
    case n: TopicConfigKeyConstraint.Regex => n.asJson
    case n: TopicConfigKeyConstraint.Prefix => n.asJson
  }
  implicit val topicConfigKeyConstraintDecoder: Decoder[TopicConfigKeyConstraint] =
    List[Decoder[TopicConfigKeyConstraint]](
      // a simple string gets decoded as an exact topic config constraint
      Decoder[String].map(TopicConfigKeyConstraint.Exact).widen,
      Decoder[TopicConfigKeyConstraint.Exact].widen,
      Decoder[TopicConfigKeyConstraint.Regex].widen,
      Decoder[TopicConfigKeyConstraint.Prefix].widen
    ).reduceLeft(_ or _)
}
