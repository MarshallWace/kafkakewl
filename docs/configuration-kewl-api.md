# **kafkakewl-api** Configuration

The **kafkakewl-api** service is configured with the usual [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) config files ([application.conf](../kewl-api/src/main/resources/application.conf)), exposing lots of environment variables to make it easier to tweak certain settings.

It's also possible to override parts of the main `application.conf` config files with the following optional files in the source root directory (or at runtime the current directory):

`.kafkakewl-api-overrides.conf`

Overriding is useful when some sections need to tbe completely custom and environment variables are cumbersome or not possible to use.

# Important configurable components

## Persistence

The **kafkakewl-api** persists the submitted topologies, deployments, deployed topologies and permissions too. In addition to just persisting these entities it populates a change-log of changes too so that other services (e.g. **kafkakewl-api-metrics**) can be notified about changes.

This change-log is always a single topic in a kafka-cluster: `kewl.changelog`

For storing entities, there are 2 options currently:

### **Kafka**

No initialization is needed, **kafkakewl-api** will create the the topics needed for this:
- A single topic: `kewl[.dev].changes.state-entities` in a kafka-cluster to store topologies, permissions.
- A topic per deployment kafka cluster: `kewl[.dev].changes.deployment-entities` to store the deployed topologies.

Note that the `.dev` part in the topic name is there only in `dev` environments, see the `KAFKAKEWL_API_ENV` environment variable.

### **Microsoft Sql-Server**

To initialize the tables/stored procedures, run the [SqlPersistentStore.sql](../setup/SqlPersistentStore.sql) script.

## Authentication

As expected, the way **kafkakewl-api** authenticates the users can be customized, by implementing the [AuthPlugin](../kewl-extensions/src/main/scala/com/mwam/kafkakewl/extensions/AuthPlugin.scala) trait. There are 3 built-in implementations in the **kewl-extensions-builtin** module:
- [SameUserAuthPlugin](../kewl-extensions-builtin/src/main/scala/com/mwam/kafkakewl/extensions/builtin/BuiltinAuthPlugins.scala): it's not a real auth plug-in because it pretends that a single hard-coded user is using the system
- [HttpHeaderAuthPlugin](../kewl-extensions-builtin/src/main/scala/com/mwam/kafkakewl/extensions/builtin/BuiltinAuthPlugins.scala): it takes the user-name from the configurable HTTP header
- [KerberosAuthPlugin](../kewl-extensions-builtin/src/main/scala/com/mwam/kafkakewl/extensions/builtin/BuiltinAuthPlugins.scala): authenticates with the usual negotiate protocol using kerberos

## Permissions

**kafkakewl** has its own permissioning systems to allow or disallow users from manipulating certain topologies or kafka-clusters. The built-in implementation is based on **Permission** entities defining who can do what, and these are also exposed on the REST API. However it's possible to choose another **Permission** provider if needed one just needs to implement the [PermissionPlugin](../kewl-extensions/src/main/scala/com/mwam/kafkakewl/extensions/PermissionPlugin.scala) trait.

# The most important configuration options

`KAFKAKEWL_API_ENV`: can be either `dev` (**default**) or `prod`. `dev` indicates that kafka-based persistence will use the topic names `kewl.dev.changes.state-entities` and `kewl.dev.changes.deployment-entities`.

In `prod` they are just `kewl.changes.state-entities` and `kewl.changes.deployment-entities`

Set it to `prod` in prod, for local development leave it on the default `dev`.

`KAFKAKEWL_API_HTTP_PORT`: the http port of the REST API, by default 8080.

`KAFKAKEWL_API_HTTP_ALLOWED_ORIGINS`: an optional comma separated list of allowed origins for CORS, by default empty.

`KAFKAKEWL_API_HTTP_AUTH_PLUGIN_NAME`: the class name of the authentication plugin, by default `com.mwam.kafkakewl.extensions.builtin.SameUserAuthPlugin`

The built-in plugins:
- **com.mwam.kafkakewl.extensions.builtin.SameUserAuthPlugin**
- **com.mwam.kafkakewl.extensions.builtin.HttpHeaderAuthPlugin**
- **com.mwam.kafkakewl.extensions.builtin.KerberosAuthPlugin**


`KAFKAKEWL_API_PERMISSION_PLUGIN_NAME`: the class name of the custom permission plugin, by default empty, in which case the default permissioning (permission entity manipulated via the REST API) will be active.

`KAFKAKEWL_API_PERSISTENT_STORE`: the persistent store to use for the entities, by default `kafka`. The other supported value is `sql`.

`KAFKAKEWL_API_METRICS_URI`: the uri of the **kafkakewl-api-metrics** service, by default `http://localhost:8090`

`KAFKAKEWL_API_BROKERS`: the comma separated list of kafka brokers where the `kewl.dev.changes.state-entities` topic will be created for persistent store `kafka` and where the `kewl.changelog` topic will be created (always).

`KAFKAKEWL_API_SECURITY_PROTOCOL`: the optional security protocol of the previous kafka brokers.

`KAFKAKEWL_API_JAAS_CONFIG`: the optional jaas config for the previous kafka brokers

`KAFKA_KEWL_SUPER_USERS`: the comma separated list of kafkakewl super-users.

`KAFKAKEWL_API_TOPIC_DEFAULT_OTHER_CONSUMER_NAMESPACES`: the json array of the default other consumer namespaces that will be used for every topic

For details see the `otherConsumerNamespaces` field in the [topics in topologies](topology/TopologyTopic.md).

`KAFKAKEWL_API_TOPIC_DEFAULT_OTHER_PRODUCER_NAMESPACES`: the json array of the default other producer namespaces that will be used for every topic

For details see the `otherProducerNamespaces` field in the [topics in topologies](topology/TopologyTopic.md).

The kafkakewl super-users can manipulate any topology, permissions, kafke-clusters and deployments. Basically they can do anything.

# Configuring the **com.mwam.kafkakewl.extensions.builtin.SameUserAuthPlugin**

`KAFKAKEWL_SAME_USER_AUTH_PLUGIN_USER_NAME`: the user-name that the authentication plug-in should provide to **kafkakewl-api**.

# Configuring the **com.mwam.kafkakewl.extensions.builtin.HttpHeaderAuthPlugin**

`KAFKAKEWL_HTTP_HEADER_AUTH_PLUGIN_HEADER_NAME`: the HTTP header name that contains the user-name.

`KAFKAKEWL_HTTP_HEADER_AUTH_PLUGIN_REJECTED_SCHEME`: optional, the scheme when the it's rejected because the HTTP header is missing, by default the HTTP header name itself.

`KAFKAKEWL_HTTP_HEADER_AUTH_PLUGIN_REJECTED_REALM`: optional, the realm when the it's rejected because the HTTP header is missing, by default empty string.

# Configuring the **com.mwam.kafkakewl.extensions.builtin.KerberosAuthPlugin**

`KAFKAKEWL_KERBEROS_AUTH_PLUGIN_LOWERCASE_PRINCIPAL`: optional, indicates whether it should lower-case the principal, by default true.

`KAFKAKEWL_KERBEROS_AUTH_PLUGIN_REMOVE_SUBSTRING`: optional, if set, it'll remove this sub-string from the principal.

`KAFKAKEWL_KERBEROS_PRINCIPAL`: the kerberos principal for the authentication - see [example](https://github.com/tresata/akka-http-spnego/blob/master/test-server/src/main/resources/application.conf#L2)

`KAFKAKEWL_KERBEROS_KEYTAB_PATH`: the path to the kerberos keytab - see [example](https://github.com/tresata/akka-http-spnego/blob/master/test-server/src/main/resources/application.conf#L3)

# Configuring the kafka client for the kafka-persistence and the kafka changelog topics

If you need to specify custom kafka client config for the persistence or the change-log topic population, you can do it by overriding the default `application.conf` with `.kafkakewl-api-overrides.conf`:

```
kafkakewl-api {
  changelog-store {
    kafka-cluster {
      brokers = "kafka-broker-1:9092"
      kafka-client-config {
        whatever-kafka-config: "abc"
      }
    }
  }

  state-command-processor {
    kafka-cluster {
      brokers = "kafka-broker-1:9092"
      kafka-client-config {
        whatever-kafka-config: "abc"
      }
    }
  }
}
```

Look at the default [application.conf](../kewl-api/src/main/resources/application.conf) and you can see what else can be overriden this way (everything really).

# The rest of the configuration options

## Environment Variables:

`WAIT_FOR_FILE_AT_STARTUP`: optional, before it starts, waits for this file to exist.

It's useful if e.g. you want to make sure that the kerberos token cache file exists.

`LOGBACK_ELASTIC_KERBEROS_PRINCIPAL`: the optional kerberos principal for the elastic-search logging.

`LOGBACK_ELASTIC_KERBEROS_KEYTAB`: the optional kerberos keytab path for the elastic-search logging.

`KAFKAKEWL_API_SQL_HOST`: the short host-name of the SQL server if the persistent store is `sql`.

`KAFKAKEWL_API_SQL_HOST_SUFFIX`: the suffix of the full host-name of the SQL server if the persistent store is `sql`.

`KAFKAKEWL_API_SQL_PORT`: the port of the SQL server if the persistent store is `sql`.

`KAFKAKEWL_API_SQL_DATABASE`: the database in the SQL server if the persistent store is `sql`.

`KAFKAKEWL_CHANGELOG_STORE_REPLICATION_FACTOR`: the optional replication factor of the `kewl.changelog` topic, by default 3.

`KAFKAKEWL_PROCESSOR_STATE_ENTITIES_REPLICATION_FACTOR`: the optional replication factor of the `kewl[.dev].changes.state-entities` topic, by default 3.

`KAFKAKEWL_PROCESSOR_DEPLOYMENT_ENTITIES_REPLICATION_FACTOR`: the optional replication factor of the `kewl[.dev].changes.deployment-entities` topic, by default 3.

`KAFKAKEWL_PROCESSOR_STATE_ENTITIES_TRANSACTIONAL_ID`: the optional transactional id of the producer of the `kewl[.dev].changes.state-entities` topic, by default `kewl.dev.processor.state-entities.transactional-id`.

`KAFKAKEWL_PROCESSOR_DEPLOYMENT_ENTITIES_TRANSACTIONAL_ID`: the optional transactional id of the producer of the `kewl[.dev].changes.deployment-entities` topics, by default `kewl.dev.processor.deployment-entities.transactional-id`.
