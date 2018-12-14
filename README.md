KafkaConnect JsonConverter
=====

This package simply inherits from the official `org.apache.kafka.connect.json.JsonConverter` and makes it compatible with Tombstone messages

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

