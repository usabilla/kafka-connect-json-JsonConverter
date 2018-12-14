package com.usabilla.kafka.connect.json;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.Map;

/**
 * A KafkaConnect JsonConverter which is compatible with Tombstone messages
 * For the official support status please check https://issues.apache.org/jira/browse/KAFKA-3832
 * This Package should be deprecated as soon as Kafka supports this feature
 */
public class JsonConverter extends org.apache.kafka.connect.json.JsonConverter {

    private boolean enableSchemas = JsonConverterConfig.SCHEMAS_ENABLE_DEFAULT;

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        // This handles a tombstone message when schemas are enabled
        if (enableSchemas && value == null) {
            return SchemaAndValue.NULL;
        }

        return super.toConnectData(topic, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        // This handles a tombstone message when schemas are enabled
        if (enableSchemas && schema == null && value == null) {
            return null;
        }

        return super.fromConnectData(topic, schema, value);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        JsonConverterConfig config = new JsonConverterConfig(configs);
        enableSchemas = config.schemasEnabled();

        super.configure(configs);
    }
}
