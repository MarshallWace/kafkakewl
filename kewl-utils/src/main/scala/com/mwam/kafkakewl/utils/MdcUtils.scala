/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.utils

import org.slf4j.MDC

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

trait MdcUtils {
  private def setMDCContextMapSafe(mdc: java.util.Map[String, String]): Unit = {
    if (mdc == null) {
      MDC.clear()
    } else {
      MDC.setContextMap(mdc)
    }
  }

  def decorateWithMDC[T, R](mdc: java.util.Map[String, String])(f: T => R): T => R = {
    t => {
      val currentMdc = MDC.getCopyOfContextMap
      setMDCContextMapSafe(mdc)
      try {
        f(t)
      } finally {
        setMDCContextMapSafe(currentMdc)
      }
    }
  }

  def getCurrentMDC: Map[String, String] = MDC.getCopyOfContextMap.asScala.toMap

  def decorateWithMDC[T, R](mdc: Map[String, Any])(f: T => R): T => R = decorateWithMDC(mdc.mapValues(v => if (v == null) "" else v.toString).asJava)(f)
  def decorateRunnableWithMDC(mdc: Map[String, Any])(runnable: Runnable): Runnable = {
    new Runnable {
      override def run(): Unit = withMDC(mdc) { runnable.run() }
    }
  }

  def decorateExecutionContextWithMDC(mdc: Map[String, Any])(ec: ExecutionContext): ExecutionContext = new ExecutionContext {
    override def execute(runnable: Runnable): Unit = {
      ec.execute(decorateRunnableWithMDC(mdc)(runnable))
    }

    override def reportFailure(cause: Throwable): Unit = withMDC(mdc) {
      ec.reportFailure(cause)
    }
  }

  def decorateWithCurrentMDC[T, R](f: T => R): T => R = decorateWithMDC(MDC.getCopyOfContextMap)(f)
  def decorateWithCurrentMDC[R](f: () => R): () => R = {
    // we must decorate the function here, not inside the resulting function's implementation, because that will be called in a different thread's context
    val decoratedFunc: Unit => R = decorateWithCurrentMDC(_ => f())
    // converting f to a no-parameter function (from a single-Unit parameter one)
    () => decoratedFunc(())
  }

  def withMDC[R](mdc: Map[String, Any])(r: => R): R = decorateWithMDC(mdc)((_: Unit) => r)(())
}
