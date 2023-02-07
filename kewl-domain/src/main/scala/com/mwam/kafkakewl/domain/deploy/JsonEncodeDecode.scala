/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package deploy

import java.time.OffsetDateTime

import cats.syntax.functor._
import com.mwam.kafkakewl.domain.deploy.DeployedTopology.ResetOptions
import com.mwam.kafkakewl.domain.kafkacluster._
import com.mwam.kafkakewl.domain.topology.TopicId
import io.circe.{Decoder, Encoder}
import io.circe.generic.extras.auto._
import io.circe.generic.extras.semiauto._
import io.circe.syntax._

object JsonEncodeDecode {
  import JsonEncodeDecodeBase._
  import topology.JsonEncodeDecode._

  implicit val deploymentAllowUnsafeKafkaClusterChangeEncoder: Encoder[DeploymentAllowUnsafeKafkaClusterChange] = deriveConfiguredEncoder
  implicit val deploymentAllowUnsafeKafkaClusterChangeDecoder: Decoder[DeploymentAllowUnsafeKafkaClusterChange] = deriveConfiguredDecoder
  implicit val unsafeKafkaClusterChangeOperationEncoder: Encoder[UnsafeKafkaClusterChangeOperation] = deriveEnumerationEncoder
  implicit val unsafeKafkaClusterChangeOperationDecoder: Decoder[UnsafeKafkaClusterChangeOperation] = deriveEnumerationDecoder
  implicit val kafkaClusterItemEntityTypeEncoder: Encoder[KafkaClusterItemEntityType] = deriveEnumerationEncoder
  implicit val kafkaClusterItemEntityTypeDecoder: Decoder[KafkaClusterItemEntityType] = deriveEnumerationDecoder
  implicit val deploymentOptionsEncoder: Encoder[DeploymentOptions] = deriveConfiguredEncoder
  implicit val deploymentOptionsDecoder: Decoder[DeploymentOptions] = deriveConfiguredDecoder

  /**
    * Deployed-topology related types
    */

  // TopicPartitionPosition
  private final case class TopicPartitionPositionDefaultForJson(default: Boolean)
  private final case class TopicPartitionPositionBeginningForJson(beginning: Boolean)
  private final case class TopicPartitionPositionEndForJson(end: Boolean)
  private final case class TopicPartitionPositionOffsetForJson(offset: Long)
  private final case class TopicPartitionPositionTimeStampForJson(timeStamp: OffsetDateTime)
  implicit val topicPartitionPositionEncoder: Encoder[TopicPartitionPosition] = Encoder.instance {
    case _: TopicPartitionPosition.Default => TopicPartitionPositionDefaultForJson(true).asJson
    case _: TopicPartitionPosition.Beginning => TopicPartitionPositionBeginningForJson(true).asJson
    case _: TopicPartitionPosition.End => TopicPartitionPositionEndForJson(true).asJson
    case n: TopicPartitionPosition.Offset => TopicPartitionPositionOffsetForJson(n.value).asJson
    case n: TopicPartitionPosition.TimeStamp => TopicPartitionPositionTimeStampForJson(n.value).asJson
  }
  implicit val topicPartitionPositionDecoder: Decoder[TopicPartitionPosition] =
    List[Decoder[TopicPartitionPosition]](
      // a little hack do decode { "beginning": true } to TopicPartitionPosition.Beginning and fail on anything else
      Decoder[TopicPartitionPositionBeginningForJson].flatMap(a => if (a.beginning) Decoder[TopicPartitionPosition.Beginning].widen else Decoder.failedWithMessage("does not matter")),
      // a little hack do decode { "end": true } to TopicPartitionPosition.Beginning and fail on anything else
      Decoder[TopicPartitionPositionEndForJson].flatMap(a => if (a.end) Decoder[TopicPartitionPosition.End].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[TopicPartitionPositionOffsetForJson].map(_.offset).map(TopicPartitionPosition.Offset).widen,
      Decoder[TopicPartitionPositionTimeStampForJson].map(_.timeStamp).map(TopicPartitionPosition.TimeStamp).widen,
      Decoder[TopicPartitionPositionDefaultForJson].flatMap(a => if (a.default) Decoder[TopicPartitionPosition.Default].widen else Decoder.failedWithMessage("failed to decode topic-partition position")),
    ).reduceLeft(_ or _)

  implicit val topicPartitionPositionsEncoder: Encoder[Map[TopicId, TopicPartitionPosition]] = Encoder[Map[String, TopicPartitionPosition]].contramap(_.toMapByString)
  implicit val topicPartitionPositionsDecoder: Decoder[Map[TopicId, TopicPartitionPosition]] = Decoder[Map[String, TopicPartitionPosition]].map(_.toMapByTopicId)

  implicit val topicPartitionPositionsOfPartitionEncoder: Encoder[Map[TopicId, Seq[TopicPartitionPositionOfPartition]]] = Encoder[Map[String, Seq[TopicPartitionPositionOfPartition]]].contramap(_.toMapByString)
  implicit val topicPartitionPositionsOfPartitionDecoder: Decoder[Map[TopicId, Seq[TopicPartitionPositionOfPartition]]] = Decoder[Map[String, Seq[TopicPartitionPositionOfPartition]]].map(_.toMapByTopicId)

  implicit val resetOptionsEncoder: Encoder[ResetOptions] = deriveConfiguredEncoder
  implicit val resetOptionsDecoder: Decoder[ResetOptions] = deriveConfiguredDecoder

  /**
    * Other types, entities
    */

  // DeploymentChangeTopologyVersion
  private final case class DeploymentChangeTopologyVersionRemoveForJson(remove: Boolean)
  private final case class DeploymentChangeTopologyVersionLatestOnceForJson(latestOnce: Boolean)
  private final case class DeploymentChangeTopologyVersionLatestTrackingForJson(latestTracking: Boolean)
  implicit val deploymentChangeTopologyVersionEncoder: Encoder[DeploymentChangeTopologyVersion] = Encoder.instance {
    case _: DeploymentChangeTopologyVersion.Remove => DeploymentChangeTopologyVersionRemoveForJson(true).asJson
    case _: DeploymentChangeTopologyVersion.LatestOnce => DeploymentChangeTopologyVersionLatestOnceForJson(true).asJson
    case _: DeploymentChangeTopologyVersion.LatestTracking => DeploymentChangeTopologyVersionLatestTrackingForJson(true).asJson
    case n: DeploymentChangeTopologyVersion.Exact => n.asJson
  }
  implicit val deploymentChangeTopologyVersionDecoder: Decoder[DeploymentChangeTopologyVersion] =
    List[Decoder[DeploymentChangeTopologyVersion]](
      Decoder[DeploymentChangeTopologyVersionRemoveForJson].flatMap(a => if (a.remove) Decoder[DeploymentChangeTopologyVersion.Remove].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[DeploymentChangeTopologyVersionLatestOnceForJson].flatMap(a => if (a.latestOnce) Decoder[DeploymentChangeTopologyVersion.LatestOnce].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[DeploymentChangeTopologyVersionLatestTrackingForJson].flatMap(a => if (a.latestTracking) Decoder[DeploymentChangeTopologyVersion.LatestTracking].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[DeploymentChangeTopologyVersion.Exact].widen
    ).reduceLeft(_ or _)

  // DeploymentTopologyVersion
  implicit val deploymentTopologyVersionEncoder: Encoder[DeploymentTopologyVersion] = Encoder.instance {
    case _: DeploymentTopologyVersion.Remove => DeploymentChangeTopologyVersionRemoveForJson(true).asJson
    case _: DeploymentTopologyVersion.LatestTracking => DeploymentChangeTopologyVersionLatestTrackingForJson(true).asJson
    case n: DeploymentTopologyVersion.Exact => n.asJson
  }
  implicit val deploymentTopologyVersionDecoder: Decoder[DeploymentTopologyVersion] =
    List[Decoder[DeploymentTopologyVersion]](
      Decoder[DeploymentChangeTopologyVersionRemoveForJson].flatMap(a => if (a.remove) Decoder[DeploymentTopologyVersion.Remove].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[DeploymentChangeTopologyVersionLatestTrackingForJson].flatMap(a => if (a.latestTracking) Decoder[DeploymentTopologyVersion.LatestTracking].widen else Decoder.failedWithMessage("does not matter")),
      Decoder[DeploymentTopologyVersion.Exact].widen
    ).reduceLeft(_ or _)

  // entities
  implicit val nonKewlKafkaAclEncoder: Encoder[NonKewlKafkaAcl] = deriveConfiguredEncoder
  implicit val nonKewlKafkaAclDecoder: Decoder[NonKewlKafkaAcl] = deriveConfiguredDecoder
  implicit val nonKewlKafkaResourcesEncoder: Encoder[NonKewlKafkaResources] = deriveConfiguredEncoder
  implicit val nonKewlKafkaResourcesDecoder: Decoder[NonKewlKafkaResources] = deriveConfiguredDecoder
  implicit val deploymentChangeEncoder: Encoder[DeploymentChange] = deriveConfiguredEncoder
  implicit val deploymentChangeDecoder: Decoder[DeploymentChange] = deriveConfiguredDecoder
  implicit val deploymentEncoder: Encoder[Deployment] = deriveConfiguredEncoder
  implicit val deploymentDecoder: Decoder[Deployment] = deriveConfiguredDecoder
  implicit val deployedTopologyStatusEncoder: Encoder[DeployedTopology] = encoderWithoutDefaultLabelled(deriveConfiguredEncoder)
  implicit val deployedTopologyStatusDecoder: Decoder[DeployedTopology] = deriveConfiguredDecoder

  // entity state changes
  implicit val deploymentEntityStateChangeEncoder: Encoder[DeploymentStateChange.StateChange] = deriveConfiguredEncoder
  implicit val deploymentEntityStateChangeDecoder: Decoder[DeploymentStateChange.StateChange] = deriveConfiguredDecoder
  implicit val deployedTopologyEntityStateChangeEncoder: Encoder[DeployedTopologyStateChange.StateChange] = deriveConfiguredEncoder
  implicit val deployedTopologyEntityStateChangeDecoder: Decoder[DeployedTopologyStateChange.StateChange] = deriveConfiguredDecoder
}
