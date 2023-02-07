/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object ExecutorContextFactory {
  private class ThreadFactoryWithNamePrefix(namePrefix: String) extends ThreadFactory {
    // from DefaultThreadFactory, but with a parameter namePrefix

    private val group = Option(System.getSecurityManager).map(_.getThreadGroup).getOrElse(Thread.currentThread().getThreadGroup)
    private val threadNumber = new AtomicInteger(1)

    def newThread(runnable: Runnable): Thread = {
      val thread = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0)
      if (thread.isDaemon) thread.setDaemon(false)
      if (thread.getPriority != Thread.NORM_PRIORITY) thread.setPriority(Thread.NORM_PRIORITY)
      thread
    }
  }
}

trait ExecutorContextFactory {
  import ExecutorContextFactory._

  def createExecutorContext(
    config: Config,
    namePrefix: String,
  ): (ExecutionContext, Int) = {
    val executor = config.getStringOrNoneIfEmpty("executor").getOrElse("thread-pool-executor")
    executor match {
      case "thread-pool-executor" =>
        val numberOfThreads = config.getIntOrNoneIfEmpty("thread-pool-executor.fixed-pool-size").getOrElse(16)
        (
          createThreadPoolExecutorService(numberOfThreads, namePrefix),
          numberOfThreads
        )

      case _ =>
        throw new RuntimeException(s"Unknown executor '$executor', possible values: 'thread-pool-executor'.")
    }
  }

  def createThreadPoolExecutorService(
    numberOfThreads: Int,
    namePrefix: String,
  ): ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(
        new ThreadPoolExecutor(
          numberOfThreads,
          numberOfThreads,
          0,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue[Runnable](),
          new ThreadFactoryWithNamePrefix(namePrefix)
        )
      )
}
