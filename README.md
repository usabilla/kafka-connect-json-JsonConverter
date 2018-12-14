KafkaConnect JsonConverter
=====

This package simply inherits from the official `org.apache.kafka.connect.json.JsonConverter` and makes it compatible with Tombstone messages

## The problem

Currently the official JsonConverter when schemas is enabled, it won't be able to generate tombstone messages, when you attempt to send a `NULL` record, the output looks like:

```json
{
  "schema": null,
  "payload": null
}
```

Which is an invalid tombstone message for a kafka topic with compaction, this package allows you to produce proper tombstones meanwhile you have your schemas enabled.

More about log compaction and tombstones:

http://cloudurable.com/blog/kafka-architecture-log-compaction/index.html

## Official support by kafka

This issue is currently open and being discussed within Kafka https://issues.apache.org/jira/browse/KAFKA-3832

Feel free to use this package if you need early support

## Usage

You have to build from source and copy the jar towards your Kafka Connect plugins directory

```console
$ gradle
$ gradlew jar
```

### Replace the connector configuration with this one

In your connector json configuration:

```json
{
...
    "key.converter": "com.usabilla.kafka.connect.json.JsonConverter",
    "value.converter": "com.usabilla.kafka.connect.json.JsonConverter",
...
}
```

Documentation about connectors configuration https://docs.confluent.io/current/connect/managing/configuring.html#configuring-connectors

