/*
 * SPDX-FileCopyrightText: 2023 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package com.mwam.kafkakewl.domain

import org.scalatest.WordSpec

class ExpressionsSpec extends WordSpec {
  "an empty string" when {
    "there are no variables" should {
      "be resolved to an empty string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("", variables, "test").right.get
        assert(resolved == Seq(""))
      }
    }

    "there are some variables" should {
      "be resolved to an empty string" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("", variables, "test").right.get
        assert(resolved == Seq(""))
      }
    }
  }

  "a constant string" when {
    "there are no variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("a b string with no variables", variables, "test").right.get
        assert(resolved == Seq("a b string with no variables"))
      }
    }

    "there are some variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("a b string with no variables", variables, "test").right.get
        assert(resolved == Seq("a b string with no variables"))
      }
    }
  }

  "a constant string containing escaped characters" when {
    "there are no variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("a b \\$ \\str\\ing \\\\ with no \\${ variables", variables, "test").right.get
        assert(resolved == Seq("a b $ string \\ with no ${ variables"))
      }
    }
  }

  "a constant string ending with an escaped character" when {
    "there are no variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("a b string with no variables\\\\", variables, "test").right.get
        assert(resolved == Seq("a b string with no variables\\"))
      }
    }
  }

  "a constant string ending with an unfinished escaped character" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("a b string with no variables\\", variables, "test").left.get
        assert(errors.contains(s"'test' cannot end with an unfinished escaping character"))
      }
    }
  }

  "a constant string ending with an unfinished empty variable" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("a b string with no variables${", variables, "test").left.get
        assert(errors.contains(s"'test' cannot end with an unfinished variable"))
      }
    }
  }

  "a constant string ending with an unfinished variable" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("a b string with no variables${a", variables, "test").left.get
        assert(errors.contains("'test' cannot end with an unfinished variable"))
      }
    }
  }

  "a constant string with unescaped $ not followed by {" when {
    "there are no variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("prefix $something suffix", variables, "test").right.get
        assert(resolved == Seq("prefix $something suffix"))
      }
    }
  }

  "a constant string ending with unescaped $ not followed by {" when {
    "there are no variables" should {
      "be resolved to the string" in {
        val variables = Expressions.Variables.empty
        val resolved = Expressions.resolveVariables("prefix $", variables, "test").right.get
        assert(resolved == Seq("prefix $"))
      }
    }
  }

  "a string with a variable only" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("${a}", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variable is defined as a single string" should {
      "be resolved to the variable value" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("${a}", variables, "test").right.get
        assert(resolved == Seq("something"))
      }
    }

    "the variable is defined as a multiple strings" should {
      "be resolved to the variable values" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("${b}", variables, "test").right.get
        assert(resolved == Seq("1", "2"))
      }
    }
  }

  "a string with a variable and some prefix and no suffix" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("prefix ${a}", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variable is defined as a single string" should {
      "be resolved to the variable value with the prefix only" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${a}", variables, "test").right.get
        assert(resolved == Seq("prefix something"))
      }
    }

    "the variable is defined as a multiple strings" should {
      "be resolved to the variable values with the prefix only" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${b}", variables, "test").right.get
        assert(resolved == Seq("prefix 1", "prefix 2"))
      }
    }
  }

  "a string with a variable and some prefix and suffix" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("prefix ${a} suffix", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variable is defined as a single string" should {
      "be resolved to the variable value with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${a} suffix", variables, "test").right.get
        assert(resolved == Seq("prefix something suffix"))
      }
    }

    "the variable is defined as a multiple strings" should {
      "be resolved to the variable values with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${b} suffix", variables, "test").right.get
        assert(resolved == Seq("prefix 1 suffix", "prefix 2 suffix"))
      }
    }
  }

  "a string with a variable and no prefix and some suffix" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("${a} suffix", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variable is defined as a single string" should {
      "be resolved to the variable value with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("${a} suffix", variables, "test").right.get
        assert(resolved == Seq("something suffix"))
      }
    }

    "the variable is defined as a multiple strings" should {
      "be resolved to the variable values with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("${b} suffix", variables, "test").right.get
        assert(resolved == Seq("1 suffix", "2 suffix"))
      }
    }
  }

  "a string with multiple variables and some prefix and suffix" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("prefix ${a} middle ${b} suffix", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variables are defined" should {
      "be resolved to the variable values with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${a} middle ${b} suffix", variables, "test").right.get
        assert(resolved == Seq("prefix something middle 1 suffix", "prefix something middle 2 suffix"))
      }
    }
  }

  "a string with escaped $, multiple variables and some prefix and suffix" when {
    "there are no variables" should {
      "be invalid" in {
        val variables = Expressions.Variables.empty
        val errors = Expressions.resolveVariables("prefix ${a} middle \\${x ${b} suffix", variables, "test").left.get
        assert(errors.contains("variable 'a' is not defined in 'test'"))
      }
    }

    "the variables are defined" should {
      "be resolved to the variable values with the prefix and suffix" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${a} middle \\${x ${b} suffix", variables, "test").right.get
        assert(resolved == Seq("prefix something middle ${x 1 suffix", "prefix something middle ${x 2 suffix"))
      }
    }
  }

  "a string with a variable containing $" when {
    "there are some variables" should {
      "be resolved to the variable values correctly" in {
        val variables = Expressions.Variables("a$b" -> Seq("something"), "b$a" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix ${a$b}", variables, "test").right.get
        assert(resolved == Seq("prefix something"))
      }
    }
  }

  "a string with a variable preceded by an escaped $" when {
    "there are some variables" should {
      "be resolved to the variable values correctly" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix \\$${a}", variables, "test").right.get
        assert(resolved == Seq("prefix $something"))
      }
    }
  }

  "a string with a variable preceded by an unescaped $" when {
    "there are some variables" should {
      "be resolved to the variable values correctly" in {
        val variables = Expressions.Variables("a" -> Seq("something"), "b" -> Seq("1", "2"))
        val resolved = Expressions.resolveVariables("prefix $${a}", variables, "test").right.get
        assert(resolved == Seq("prefix $something"))
      }
    }
  }
}
