/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common

import java.util.ServiceLoader

import com.mwam.kafkakewl.extensions.Plugin

import scala.collection.JavaConverters._

object PluginLoader {
  type LoadedPlugins[P <: Plugin] = Map[String, P]

  def loadPlugins[P <: Plugin]()(implicit ctag: reflect.ClassTag[P]): LoadedPlugins[P] = {
    val serviceLoader = ServiceLoader.load(ctag.runtimeClass.asInstanceOf[Class[P]])
    serviceLoader.asScala.map(p => (p.name, p)).toMap
  }
}
