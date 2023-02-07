# FlexibleId

Flexible ids can define some kind of matching expression that can match zero or more ids. They can be used:
- in [aliases](/topology/TopologyAliases.md) to specify a set of topic or application identifiers that the alias points to

Their forms and matching rules are similar but slightly different from [FlexibleNames](../docs/FlexibleName.md).

## String literals
It matches only exactly that identifier or it makes it fully qualified one in the current topology and matches that.

Examples:

if it's used in topology `projectx` then `"topic1"` or `"projectx.topic1"` matches the same (local) topic.

if it's used in topology `projectx` then `"projecty.topic1"` matches `"projectx.projecty.topic1"` and `"projecty.topic1"`. The former most likely doesn't exist, so it'll match only `"projecty.topic1"` which is an external topic's fully-qualified identifier.

You can specify local topic/application identifiers only with the local name, but you need to use the fully-qualified identifiers for external topics/applications (fully-qualified identifiers work for local ones too).

## Prefix
Matches every **fully-qualified** identifier that has the specified prefix, case sensitively.

Example: `{ "prefix": "project" }` matches any fully-qualified identifier that starts with `"project"`

Note that it's NOT an error if it does not match anything!

## Regex
Matches every **fully-qualified** identifier that matches the whole-string (`^` and `$` added if does not exist) regex. For instance, if you specify a string literal without any regex characters in it, it's like exact-matching. If you want containment, you need to explicitly specify `.*` before/after.

Example: `{ "regex": "projectx.*topic" }` matches any name that starts with `"projectx"` and ends with `"topic"`

Note that it's NOT an error if it does not match anything!

## Namespace
Matches every **fully-qualified** identifier that's part of the case-sensitive namespace. It's almost the same as prefix matching, but it either matches on equal strings or when the other string starts with `namespace + "."`

Example: `{ "namespace": "projectx" }` matches any fully-qualified identifier that's either exactly `"projectx"` or starts with `"projectx."`

Note that it's NOT an error if it does not match anything!
