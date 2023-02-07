/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.api.metrics.kafkacluster.observers.lag

import akka.actor.{Actor, ActorRef, Props, Terminated}
import akka.routing.{ActorRefRoutee, Router, SmallestMailboxRoutingLogic}
import cats.syntax.option._
import com.mwam.kafkakewl.common.ActorPreRestartLog
import com.mwam.kafkakewl.domain.Mdc
import com.mwam.kafkakewl.domain.kafkacluster.KafkaClusterEntityId
import com.mwam.kafkakewl.kafka.utils.KafkaConnection
import com.mwam.kafkakewl.utils.MdcUtils
import com.typesafe.scalalogging.LazyLogging
import nl.grons.metrics4.scala.DefaultInstrumented

import java.util.UUID

object ConsumeAfterConsumerGroupOffsetRouterActor {
  def props(
    kafkaClusterId: KafkaClusterEntityId,
    numberOfWorkers: Int,
    pollDuration: java.time.Duration
  ): Props = Props(
    new ConsumeAfterConsumerGroupOffsetRouterActor(kafkaClusterId, numberOfWorkers, pollDuration)
  )

  def name(kafkaClusterId: KafkaClusterEntityId) = s"ConsumeAfterConsumerGroupOffsetRouterActor-$kafkaClusterId-${UUID.randomUUID().toString.replace("-", "")}"
}

class ConsumeAfterConsumerGroupOffsetRouterActor(
  kafkaClusterId: KafkaClusterEntityId,
  numberOfWorkers: Int,
  pollDuration: java.time.Duration
) extends Actor
  with ActorPreRestartLog
  with DefaultInstrumented
  with LazyLogging
  with MdcUtils
{
  import ConsumeAfterConsumerGroupOffsetActor._

  override val actorMdc: Map[String, String] = Mdc.fromKafkaClusterId(kafkaClusterId)

  var kafkaConnection: Option[KafkaConnection] = none

  var router: Router = {
    val workers = Vector.fill(numberOfWorkers) { ActorRefRoutee(createWorker()) }
    Router(SmallestMailboxRoutingLogic(), workers)
  }

  private def createWorker(): ActorRef = {
    val worker = context.actorOf(
      ConsumeAfterConsumerGroupOffsetActor.props(kafkaClusterId, pollDuration),
      ConsumeAfterConsumerGroupOffsetActor.name(kafkaClusterId)
    )
    context.watch(worker)
    worker
  }

  override def aroundReceive(receive: Actor.Receive, msg: Any): Unit = {
    withMDC(actorMdc) {
      super.aroundReceive(receive, msg)
    }
  }

  override def receive: Receive = {
    case Terminated(terminatedWorker) =>
      // replacing the old worker with a new one
      router = router.removeRoutee(terminatedWorker)
      val worker = createWorker()
      kafkaConnection.foreach(worker ! SetKafkaConnection(_))
      router = router.addRoutee(worker)

    case setKafkaConnection @ SetKafkaConnection(kafkaConnection) =>
      this.kafkaConnection = kafkaConnection.some
      // the kafka-connection must go to all workers
      router.routees.foreach(_.send(setKafkaConnection, sender()))

    case consumeAfterConsumerGroupOffset: ConsumeAfterConsumerGroupOffset =>
      router.route(consumeAfterConsumerGroupOffset, sender())
  }
}
