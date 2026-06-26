/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.common.http

import org.scalatest.{FlatSpec, Matchers}

class ReadOnlyHttpAuthorizerSpec extends FlatSpec with Matchers {

  "an authorizer with no regex configured" should "reject nothing" in {
    val a = new ReadOnlyHttpAuthorizer(None)
    a.isRejected("readonly-alice", "POST") shouldBe false
    a.isRejected("readonly-alice", "GET") shouldBe false
  }

  "an authorizer restricting matching users to read-only" should "reject them on mutating methods" in {
    val a = new ReadOnlyHttpAuthorizer(Some("readonly-.+"))
    a.isRejected("readonly-alice", "POST") shouldBe true
    a.isRejected("readonly-alice", "PUT") shouldBe true
    a.isRejected("readonly-alice", "PATCH") shouldBe true
    a.isRejected("readonly-alice", "DELETE") shouldBe true
  }

  it should "allow matching users on read-only methods" in {
    val a = new ReadOnlyHttpAuthorizer(Some("readonly-.+"))
    a.isRejected("readonly-alice", "GET") shouldBe false
    a.isRejected("readonly-alice", "HEAD") shouldBe false
    a.isRejected("readonly-alice", "OPTIONS") shouldBe false
  }

  it should "allow non-matching users on any method" in {
    val a = new ReadOnlyHttpAuthorizer(Some("readonly-.+"))
    a.isRejected("alice", "POST") shouldBe false
    a.isRejected("bob", "DELETE") shouldBe false
  }

  "method matching" should "be case-insensitive on the request method" in {
    val a = new ReadOnlyHttpAuthorizer(Some("readonly-.+"))
    a.isRejected("readonly-alice", "post") shouldBe true
    a.isRejected("readonly-alice", "get") shouldBe false
  }

  "an anchored user regex" should "only reject exact matches" in {
    val a = new ReadOnlyHttpAuthorizer(Some("^restricted$"))
    a.isRejected("restricted", "POST") shouldBe true
    a.isRejected("restricted-user", "POST") shouldBe false
  }
}
