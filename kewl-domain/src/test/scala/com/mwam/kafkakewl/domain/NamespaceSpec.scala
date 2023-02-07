/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import com.mwam.kafkakewl.domain.topology.{Namespace, TopologyId}
import org.scalatest.FlatSpec

class NamespaceSpec extends FlatSpec {
  "empty namespace" should "contain anything" in {
    val emptyNamespace = Namespace("")

    assert(emptyNamespace.contains(""))
    assert(emptyNamespace.contains("something"))
    assert(emptyNamespace.contains("project.package.folder"))
    assert(emptyNamespace.contains("."))
  }

  "a single fragment namespace" should "contain the right names only" in {
    val namespace = Namespace("project")

    assert(namespace.contains("project.package"))
    assert(namespace.contains("project.package.folder"))

    assert(!namespace.contains("project"))
    assert(!namespace.contains("project2"))
    assert(!namespace.contains("projectpackage"))
    assert(!namespace.contains("project-package"))
  }

  "empty namespace and empty topology id" should "produce an empty topology entity id" in {
    assert(Namespace(Namespace(""), TopologyId("")).isEmpty)
  }

  "empty namespace and non-empty topology id" should "produce the topology id as topology entity id" in {
    assert(Namespace(Namespace(""), TopologyId("topology1")) == Namespace("topology1"))
  }

  "non-empty namespace and empty topology id" should "produce the namespace as topology entity id" in {
    assert(Namespace(Namespace("project.package"), TopologyId("")) == Namespace("project.package"))
  }

  "non-empty namespace and non-empty topology id" should "combine them as topology entity id" in {
    assert(Namespace(Namespace("project.package"), TopologyId("topology1")) == Namespace("project.package.topology1"))
  }
}
