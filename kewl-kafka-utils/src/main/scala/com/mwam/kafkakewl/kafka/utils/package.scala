/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.kafka

import cats.data.Writer
import cats.instances.vector._
import com.mwam.kafkakewl.utils._
import com.typesafe.config.Config
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.common.ConsumerGroupState

package object utils {
  /**
    * A type to represent kafka operations' results, with the possibility of logs next to the value (A Writer ).
    * @tparam T the result type
    */
  type KafkaOperationResult[T] = Writer[Vector[String], T]

  object KafkaOperationResult {
    def empty: KafkaOperationResult[Unit] = Writer.value[Vector[String], Unit](())
    def value[V](v: V): KafkaOperationResult[V] = Writer.value[Vector[String], V](v)
    def tell(s: String): KafkaOperationResult[Unit] = Writer.tell(Vector(s))
    def tell(s: Vector[String]): KafkaOperationResult[Unit] = Writer.tell(s)
  }

  implicit class CommonConfigExtensions(config: Config) {
    def kafkaConnectionInfo(path: String): Option[KafkaConnectionInfo] = {
      val pathPrefix = if (path.nonEmpty) s"$path." else ""
      for {
        brokers <- config.getStringOrNoneIfEmpty(s"${pathPrefix}brokers")
        securityProtocolOrNone = config.getStringOrNoneIfEmpty(s"${pathPrefix}security-protocol")
        kafkaConfig = config.kafkaConfig(s"${pathPrefix}kafka-client-config")
      } yield KafkaConnectionInfo(brokers, securityProtocolOrNone, kafkaConfig)
    }

    def kafkaConnection(path: String): Option[KafkaConnection] = {
      val pathPrefix = if (path.nonEmpty) s"$path." else ""
      for {
        info <- kafkaConnectionInfo(path)
        jaasConfigOrNone = config.getStringOrNoneIfEmpty(s"${pathPrefix}jaas-config")
      } yield KafkaConnection(info, jaasConfigOrNone)
    }

    def kafkaConnectionOrNone(path: String): Option[KafkaConnection] = if (config.hasPath(path)) kafkaConnection(path) else None
  }

  def ensureConsumerGroupDescriptionIsNotLive(consumerGroupDescription: ConsumerGroupDescription): Either[String, Unit] = {
    consumerGroupDescription.state() match {
      // only allowing to reset offsets of consumer groups which are NOT stable/re-balancing
      case ConsumerGroupState.EMPTY | ConsumerGroupState.DEAD if consumerGroupDescription.members().isEmpty => Right(())
      case state => Left(s"consumer group ${consumerGroupDescription.groupId} is $state and has members '${consumerGroupDescription.members()}': make sure you stop all running instances of your application")
    }
  }
}
