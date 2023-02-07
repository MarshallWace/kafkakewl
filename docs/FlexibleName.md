# FlexibleName

Flexible names can define some kind of matching expression that can match zero or more names. The types of the names they can match depends on where they are used:
- in [permissions](../docs/permission/Permission.md) to refer to a set of resources (e.g. a namespace or a topologies with a particular prefix, concrete kafka-cluster identifiers)
- in [topics](/topology/TopologyTopic.md) to specify a set of other namespaces that the topic is exposed to (e.g. any namespace or namespaces in a particular parent namespace)
- in [applications](/topology/TopologyApplication.md) to specify a set of other namespaces that the application is exposed to (e.g. any namespace or namespaces in a particular parent namespace)

## String literals
It matches only exactly that string, case sensitively.

Example: `"projectx.topic1"` matches `"projectx.topic1"` only

## Prefix
Matches every string that has the specified prefix, case sensitively.

Example: `{ "prefix": "project" }` matches any name that starts with `"project"`

## Regex
Matches every string that matches the whole-string (`^` and `$` added if does not exist) regex. For instance, if you specify a string literal without any regex characters in it, it's the same as exact matching. If you want containment, you need to explicitly specify `.*` before/after.

Example: `{ "regex": "projectx.*topic" }` matches any name that starts with `"projectx"` and ends with `"topic"`

## Namespace
Matches every string that's part of the case-sensitive namespace. It's almost the same as prefix matching, but it either matches on equal strings or when the other string starts with `namespace + "."`

Example: `{ "namespace": "projectx" }` matches any name that's either exactly `"projectx"` or starts with `"projectx."`

## Any
Matches anything and everything.

Example `{ "any": true }`
