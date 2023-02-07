/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

package topology

import org.scalatest.FlatSpec

class FlexibleNodeIdSpec extends FlatSpec {
  "FlexibleNodeId.Exact" should "match case-sensitively" in {
    assert(FlexibleNodeId.Exact("local-name").doesMatch("local-name"))
    assert(!FlexibleNodeId.Exact("Local-Name").doesMatch("local-name"))
  }

  "FlexibleNodeId.Exact without a topology namespace" should "match only fully qualified" in {
    assert(FlexibleNodeId.Exact("test.local-name").doesMatch("test.local-name"))
    assert(!FlexibleNodeId.Exact("local-name").doesMatch("test.local-name"))
  }

  "FlexibleNodeId.Exact with a topology namespace" should "match both local and fully qualified" in {
    assert(FlexibleNodeId.Exact("test.local-name").withTopologyNamespace(Namespace("test")).doesMatch("test.local-name"))
    assert(FlexibleNodeId.Exact("local-name").withTopologyNamespace(Namespace("test")).doesMatch("test.local-name"))
  }
}
