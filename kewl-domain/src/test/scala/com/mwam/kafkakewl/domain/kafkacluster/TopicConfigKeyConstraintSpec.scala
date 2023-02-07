/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain.kafkacluster

import org.scalatest.{FlatSpec, Matchers}

class TopicConfigKeyConstraintSpec extends FlatSpec with Matchers {
  "an empty TopicConfigKeyConstraintInclusive" should "include everything" in {
    val tckc = TopicConfigKeyConstraintInclusive()
    Seq("", "a", "aa", "b", "ba", "bab", "c", "ccc", "c1c", "c9c")
      .foreach {
        configKey => tckc.isIncluded(configKey) shouldBe true
      }
  }

  "a TopicConfigKeyConstraintInclusive with includes only" should "work correctly" in {
    val tckc = TopicConfigKeyConstraintInclusive(
      include = Seq(
        TopicConfigKeyConstraint.Exact("a"),
        TopicConfigKeyConstraint.Prefix("b"),
        TopicConfigKeyConstraint.Regex("c\\d.*")
      )
    )
    tckc.isIncluded("") shouldBe false
    tckc.isIncluded("a") shouldBe true
    tckc.isIncluded("aa") shouldBe false
    tckc.isIncluded("b") shouldBe true
    tckc.isIncluded("ba") shouldBe true
    tckc.isIncluded("bab") shouldBe true
    tckc.isIncluded("c") shouldBe false
    tckc.isIncluded("ccc") shouldBe false
    tckc.isIncluded("c1c") shouldBe true
    tckc.isIncluded("c9c") shouldBe true
  }

  "a TopicConfigKeyConstraintInclusive with includes and excludes" should "work correctly" in {
    val tckc = TopicConfigKeyConstraintInclusive(
      include = Seq(
        TopicConfigKeyConstraint.Exact("a"),
        TopicConfigKeyConstraint.Prefix("b"),
        TopicConfigKeyConstraint.Regex("c\\d.*")
      ),
      exclude = Seq(
        TopicConfigKeyConstraint.Regex("ba.+"),
        TopicConfigKeyConstraint.Regex("c9.*")
      )
    )
    tckc.isIncluded("") shouldBe false
    tckc.isIncluded("a") shouldBe true
    tckc.isIncluded("aa") shouldBe false
    tckc.isIncluded("b") shouldBe true
    tckc.isIncluded("ba") shouldBe true
    tckc.isIncluded("bab") shouldBe false
    tckc.isIncluded("c") shouldBe false
    tckc.isIncluded("ccc") shouldBe false
    tckc.isIncluded("c1c") shouldBe true
    tckc.isIncluded("c9c") shouldBe false
  }

  "a TopicConfigKeyConstraintInclusive with excludes only" should "work correctly" in {
    val tckc = TopicConfigKeyConstraintInclusive(
      exclude = Seq(
        TopicConfigKeyConstraint.Regex("ba.+"),
        TopicConfigKeyConstraint.Regex("c9.*")
      )
    )
    tckc.isIncluded("") shouldBe true
    tckc.isIncluded("a") shouldBe true
    tckc.isIncluded("aa") shouldBe true
    tckc.isIncluded("b") shouldBe true
    tckc.isIncluded("ba") shouldBe true
    tckc.isIncluded("bab") shouldBe false
    tckc.isIncluded("c") shouldBe true
    tckc.isIncluded("ccc") shouldBe true
    tckc.isIncluded("c1c") shouldBe true
    tckc.isIncluded("c9c") shouldBe false
  }

  "an empty TopicConfigKeyConstraintExclusive" should "include nothing" in {
    val tckc = TopicConfigKeyConstraintExclusive()
    Seq("", "a", "aa", "b", "ba", "bab", "c", "ccc", "c1c", "c9c")
      .foreach {
        configKey => tckc.isIncluded(configKey) shouldBe false
      }
  }

  "a TopicConfigKeyConstraintExclusive with includes only" should "work correctly" in {
    val tckc = TopicConfigKeyConstraintExclusive(
      include = Seq(
        TopicConfigKeyConstraint.Exact("a"),
        TopicConfigKeyConstraint.Prefix("b"),
        TopicConfigKeyConstraint.Regex("c\\d.*")
      )
    )
    tckc.isIncluded("") shouldBe false
    tckc.isIncluded("a") shouldBe true
    tckc.isIncluded("aa") shouldBe false
    tckc.isIncluded("b") shouldBe true
    tckc.isIncluded("ba") shouldBe true
    tckc.isIncluded("bab") shouldBe true
    tckc.isIncluded("c") shouldBe false
    tckc.isIncluded("ccc") shouldBe false
    tckc.isIncluded("c1c") shouldBe true
    tckc.isIncluded("c9c") shouldBe true
  }

  "a TopicConfigKeyConstraintExclusive with includes and excludes" should "work correctly" in {
    val tckc = TopicConfigKeyConstraintExclusive(
      include = Seq(
        TopicConfigKeyConstraint.Exact("a"),
        TopicConfigKeyConstraint.Prefix("b"),
        TopicConfigKeyConstraint.Regex("c\\d.*")
      ),
      exclude = Seq(
        TopicConfigKeyConstraint.Regex("ba.+"),
        TopicConfigKeyConstraint.Regex("c9.*")
      )
    )
    tckc.isIncluded("") shouldBe false
    tckc.isIncluded("a") shouldBe true
    tckc.isIncluded("aa") shouldBe false
    tckc.isIncluded("b") shouldBe true
    tckc.isIncluded("ba") shouldBe true
    tckc.isIncluded("bab") shouldBe false
    tckc.isIncluded("c") shouldBe false
    tckc.isIncluded("ccc") shouldBe false
    tckc.isIncluded("c1c") shouldBe true
    tckc.isIncluded("c9c") shouldBe false
  }

  "a TopicConfigKeyConstraintExclusive with excludes only" should "include nothing" in {
    val tckc = TopicConfigKeyConstraintExclusive()
    Seq("", "a", "aa", "b", "ba", "bab", "c", "ccc", "c1c", "c9c")
      .foreach {
        configKey => tckc.isIncluded(configKey) shouldBe false
      }
  }
}
