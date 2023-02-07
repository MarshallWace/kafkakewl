/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import org.scalatest.FlatSpec

class MatcherSpec extends FlatSpec {
  def any: Matcher.Any = new Matcher.Any() {}
  def exact(exact: String, ignoreCase: Boolean): Matcher.Exact = new Matcher.Exact(exact, ignoreCase) {}
  def regex(regex: String): Matcher.WholeRegex = new Matcher.WholeRegex(regex) {}
  def prefix(prefix: String, ignoreCase: Boolean): Matcher.Prefix = new Matcher.Prefix(prefix, ignoreCase) {}
  def namespace(namespace: String, ignoreCase: Boolean): Matcher.Namespace = new Matcher.Namespace(namespace, ignoreCase) {}

  "Matcher.Any" should "match empty string" in {
    assert(any.doesMatch(""))
  }

  it should "match string with whitespace" in {
    assert(any.doesMatch(" "))
    assert(any.doesMatch("  "))
    assert(any.doesMatch("\t \t \n"))
  }

  it should "match string with special characters" in {
    assert(any.doesMatch("."))
    assert(any.doesMatch("-"))
    assert(any.doesMatch("_"))
    assert(any.doesMatch("/"))
  }

  it should "match string with words" in {
    assert(any.doesMatch("test"))
    assert(any.doesMatch("something"))
    assert(any.doesMatch("multiple.parts"))
  }

  "Matcher.Exact ignoreCase = true" should "match case-insensitive equal strings" in {
    assert(exact("Something", ignoreCase = true).doesMatch("something"))
    assert(exact("something", ignoreCase = true).doesMatch("Something"))
  }

  "Matcher.Exact ignoreCase = false" should "not match case-insensitive equal strings" in {
    assert(!exact("Something", ignoreCase = false).doesMatch("something"))
    assert(!exact("something", ignoreCase = false).doesMatch("Something"))
  }

  "Matcher.Exact ignoreCase = true" should "match case-insensitive non-equal strings" in {
    assert(!exact("Something", ignoreCase = true).doesMatch(""))
    assert(!exact("something", ignoreCase = true).doesMatch(" "))
    assert(!exact("something", ignoreCase = true).doesMatch("some"))
    assert(!exact("something", ignoreCase = true).doesMatch("somethingandmore"))
  }

  "FlexibleName.Exact ignoreCase = false" should "not match case-insensitive non-equal strings" in {
    assert(!exact("Something", ignoreCase = false).doesMatch(""))
    assert(!exact("something", ignoreCase = false).doesMatch(" "))
    assert(!exact("something", ignoreCase = false).doesMatch("some"))
    assert(!exact("something", ignoreCase = false).doesMatch("somethingandmore"))
  }

  "Matcher.Regex" should "match strings based on whole regex" in {
    assert(regex("\\d\\d").doesMatch("12"))
    assert(!regex("\\d\\d").doesMatch("123"))
    assert(!regex("\\d\\d").doesMatch("1a"))
    assert(regex("test\\..*").doesMatch("test.more"))
  }

  "Matcher.Prefix ignoreCase = true" should "match with case-insensitive prefix" in {
    assert(prefix("", ignoreCase = true).doesMatch(""))
    assert(prefix("", ignoreCase = true).doesMatch("test"))
    assert(prefix("test", ignoreCase = true).doesMatch("testsomething"))
    assert(prefix("test", ignoreCase = true).doesMatch("test.something"))
    assert(prefix("test", ignoreCase = true).doesMatch("Testsomething"))
    assert(prefix("test", ignoreCase = true).doesMatch("Test.something"))
    assert(prefix("Test", ignoreCase = true).doesMatch("testsomething"))
    assert(prefix("Test", ignoreCase = true).doesMatch("test.something"))

    assert(prefix("test", ignoreCase = true).doesMatch("testsomething"))
  }

  "Matcher.Prefix ignoreCase = false" should "match with case-sensitive prefix" in {
    assert(prefix("", ignoreCase = false).doesMatch(""))
    assert(prefix("", ignoreCase = false).doesMatch("test"))
    assert(prefix("test", ignoreCase = false).doesMatch("testsomething"))
    assert(prefix("test", ignoreCase = false).doesMatch("test.something"))
    assert(!prefix("test", ignoreCase = false).doesMatch("Testsomething"))
    assert(!prefix("test", ignoreCase = false).doesMatch("Test.something"))
    assert(!prefix("Test", ignoreCase = false).doesMatch("testsomething"))
    assert(!prefix("Test", ignoreCase = false).doesMatch("test.something"))

    assert(prefix("test", ignoreCase = true).doesMatch("testsomething"))
  }

  "Matcher.Prefix ignoreCase = true" should "not match without case-insensitive prefix" in {
    assert(!prefix("test", ignoreCase = true).doesMatch("tes"))
    assert(!prefix("test", ignoreCase = true).doesMatch(" test"))
  }

  "Matcher.Prefix ignoreCase = false" should "not match without case-insensitive prefix" in {
    assert(!prefix("test", ignoreCase = false).doesMatch("tes"))
    assert(!prefix("test", ignoreCase = false).doesMatch(" test"))
  }

  "Matcher.Namespace when it's empty" should "match anything" in {
    assert(namespace("", ignoreCase = true).doesMatch(""))
    assert(namespace("", ignoreCase = true).doesMatch("test"))
    assert(namespace("", ignoreCase = true).doesMatch("Test"))
    assert(namespace("", ignoreCase = true).doesMatch("test.more"))
    assert(namespace("", ignoreCase = true).doesMatch("Test.more"))
    assert(namespace("", ignoreCase = false).doesMatch(""))
    assert(namespace("", ignoreCase = false).doesMatch("test"))
    assert(namespace("", ignoreCase = false).doesMatch("Test"))
    assert(namespace("", ignoreCase = false).doesMatch("test.more"))
    assert(namespace("", ignoreCase = false).doesMatch("Test.more"))
  }

  "Matcher.Namespace ignoreCase = true" should "match with case-insensitive equality prefix with a dot" in {
    assert(namespace("test", ignoreCase = true).doesMatch("test"))
    assert(namespace("Test", ignoreCase = true).doesMatch("test"))
    assert(namespace("test", ignoreCase = true).doesMatch("Test"))
    assert(namespace("test", ignoreCase = true).doesMatch("test.more"))
    assert(namespace("Test", ignoreCase = true).doesMatch("test.more"))
    assert(namespace("test", ignoreCase = true).doesMatch("Test.more"))
  }

  "Matcher.Namespace ignoreCase = false" should "match with case-sensitive equality prefix with a dot" in {
    assert(namespace("test", ignoreCase = false).doesMatch("test"))
    assert(!namespace("Test", ignoreCase = false).doesMatch("test"))
    assert(!namespace("test", ignoreCase = false).doesMatch("Test"))
    assert(namespace("test", ignoreCase = false).doesMatch("test.more"))
    assert(!namespace("Test", ignoreCase = false).doesMatch("test.more"))
    assert(!namespace("test", ignoreCase = false).doesMatch("Test.more"))
  }

  "Matcher.Namespace ignoreCase = true" should "not match without case-insensitive equality prefix with a dot" in {
    assert(!namespace("test", ignoreCase = true).doesMatch("test2"))
    assert(!namespace("Test", ignoreCase = true).doesMatch("test2"))
    assert(!namespace("test", ignoreCase = true).doesMatch("Test2"))
    assert(!namespace("Test", ignoreCase = true).doesMatch("Test2"))
    assert(!namespace("test.", ignoreCase = true).doesMatch("test"))
    assert(!namespace("Test.", ignoreCase = true).doesMatch("test"))
    assert(!namespace("test.", ignoreCase = true).doesMatch("Test"))
    assert(!namespace("test.", ignoreCase = true).doesMatch("test.more"))
    assert(!namespace("Test.", ignoreCase = true).doesMatch("test.more"))
    assert(!namespace("test.", ignoreCase = true).doesMatch("Test.more"))
  }

  "Matcher.Namespace ignoreCase = false" should "not match without case-sensitive equality prefix with a dot" in {
    assert(!namespace("test", ignoreCase = false).doesMatch("test2"))
    assert(!namespace("Test", ignoreCase = false).doesMatch("test2"))
    assert(!namespace("test", ignoreCase = false).doesMatch("Test2"))
    assert(!namespace("Test", ignoreCase = false).doesMatch("Test2"))
    assert(!namespace("test.", ignoreCase = false).doesMatch("test"))
    assert(!namespace("Test.", ignoreCase = false).doesMatch("test"))
    assert(!namespace("test.", ignoreCase = false).doesMatch("Test"))
    assert(!namespace("test.", ignoreCase = false).doesMatch("test.more"))
    assert(!namespace("Test.", ignoreCase = false).doesMatch("test.more"))
    assert(!namespace("test.", ignoreCase = false).doesMatch("Test.more"))
  }
}
