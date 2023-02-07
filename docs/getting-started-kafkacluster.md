# Getting Started - registering a kafka-cluster

First you need to register at least one kafka-cluster in order to deploy any topologies.

An example kafka-cluster json:

```json
{
	"kafkaCluster": "test",
    "brokers": "broker1:9092,broker2:9092,broker3:9092",
    "name": "test",
    "environments": {
        "default": {},
        "test": {}
    }
}
```

For more examples see [kafka-clusters](kafkacluster/KafkaCluster.md).

Once you have your kafka-cluster json, you can submit it to **kafkakewl-api** using the REST API:

`curl -XPOST localhost:8080/kafkacluster -H "Content-Type: application/json" --compressed --data @kafkacluster.json`

Note that you need have permission to update this kafka-cluster or be a super-user.

To verify that it's there:

`curl localhost:8080/kafkacluster --compressed`
