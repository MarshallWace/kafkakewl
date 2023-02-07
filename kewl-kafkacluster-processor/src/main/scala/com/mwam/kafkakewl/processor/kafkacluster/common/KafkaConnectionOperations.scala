/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.processor.kafkacluster.common

import cats.syntax.either._
import com.mwam.kafkakewl.kafka.utils.{KafkaAdminConfig, KafkaConfigProperties, KafkaConnection, KafkaConsumerConfig, KafkaProducerConfig}
import com.mwam.kafkakewl.domain.{KafkaClusterCommand, KafkaClusterCommandResult, _}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer

import scala.util.Try

trait KafkaConnectionOperations {
  val connection: KafkaConnection

  def createConsumer(groupId: Option[String], kafkaRequestTimeOutMillis: Option[Int] = None) =
    new KafkaConsumer[Array[Byte], Array[Byte]](KafkaConfigProperties.forBytesConsumer(KafkaConsumerConfig(connection, groupId, kafkaRequestTimeOutMillis = kafkaRequestTimeOutMillis)))

  def createStringConsumer(groupId: Option[String]) =
    new KafkaConsumer[String, String](KafkaConfigProperties.forStringConsumer(KafkaConsumerConfig(connection, groupId)))

  def createStringProducer() =
    new KafkaProducer[String, String](KafkaConfigProperties.forProducer(KafkaProducerConfig(connection)))

  def createAdminClient(): AdminClient =
    AdminClient.create(KafkaConfigProperties.forAdmin(KafkaAdminConfig(connection)))

  def withConsumerAndCommandError[T](groupId: Option[String])(f: KafkaConsumer[Array[Byte], Array[Byte]] => ValueOrCommandError[T]): ValueOrCommandError[T] = {
    for {
      consumer <- Try(createConsumer(groupId)).toRightOrCommandError
      result <- try f(consumer) finally Try(consumer.close()).toRightOrCommandError
    } yield result
  }

  def withConsumerAndCommandErrors[T](groupId: Option[String])(f: KafkaConsumer[Array[Byte], Array[Byte]] => ValueOrCommandErrors[T]): ValueOrCommandErrors[T] = {
    for {
      consumer <- Try(createConsumer(groupId)).toRightOrCommandErrors
      result <- try f(consumer) finally Try(consumer.close()).toRightOrCommandErrors
    } yield result
  }

  def withStringConsumerAndCommandErrors[T](groupId: Option[String])(f: KafkaConsumer[String, String] => ValueOrCommandErrors[T]): ValueOrCommandErrors[T] = {
    for {
      consumer <- Try(createStringConsumer(groupId)).toRightOrCommandErrors
      result <- try f(consumer) finally Try(consumer.close()).toRightOrCommandErrors
    } yield result
  }

  def withStringProducerAndCommandErrors[T](f: KafkaProducer[String, String] => ValueOrCommandErrors[T]): ValueOrCommandErrors[T] = {
    for {
      producer <- Try(createStringProducer()).toRightOrCommandErrors
      result <- try f(producer) finally Try(producer.close()).toRightOrCommandErrors
    } yield result
  }

  def withAdminClientAndCommandErrors[T](f: AdminClient => ValueOrCommandErrors[T]): ValueOrCommandErrors[T] = {
    for {
      adminClient <- Try(createAdminClient()).toRightOrCommandErrors
      result <- try f(adminClient) finally Try(adminClient.close()).toRightOrCommandErrors
    } yield result
  }

  def withConsumerAndFailure[T](groupId: Option[String], command: KafkaClusterCommand)(
    f: KafkaConsumer[Array[Byte], Array[Byte]] => Either[KafkaClusterCommandResult.Failed, T]
  ): Either[KafkaClusterCommandResult.Failed, T] = {
    for {
      consumer <- Try(createConsumer(groupId)).toRightOrCommandErrors.leftMap(command.failedResult)
      result <- try f(consumer) finally Try(consumer.close()).toRightOrCommandErrors.leftMap(command.failedResult)
    } yield result
  }
}

trait KafkaConnectionExtraSyntax {
  implicit class KafkaConnectionExtraSyntaxExtensions(val connection: KafkaConnection) extends KafkaConnectionOperations
}
