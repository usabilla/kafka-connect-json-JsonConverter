package com.usabilla.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class JsonConverterTest {
    private static final String TOPIC = "topic";

    private ObjectMapper objectMapper = new ObjectMapper();
    private JsonConverter converter = new JsonConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.emptyMap(), false);
    }

    // Schema metadata

    @Test
    public void testConnectSchemaMetadataTranslation() {
        // this validates the non-type fields are translated and handled properly
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, null), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, \"payload\": null }".getBytes()));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().defaultValue(true).build(), true),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"default\": true }, \"payload\": null }".getBytes()));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().required().name("bool").version(2).doc("the documentation").parameter("foo", "bar").build(), true),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"version\": 2, \"doc\": \"the documentation\", \"parameters\": { \"foo\": \"bar\" }}, \"payload\": true }".getBytes()));
    }

    // Schema types

    @Test
    public void booleanToConnect() {
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": false }".getBytes()));
    }

    @Test
    public void byteToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int8\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void shortToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int16\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void intToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int32\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void longToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" }, \"payload\": 12 }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 4398046511104L), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" }, \"payload\": 4398046511104 }".getBytes()));
    }

    @Test
    public void floatToConnect() {
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.34f), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"float\" }, \"payload\": 12.34 }".getBytes()));
    }

    @Test
    public void doubleToConnect() {
        assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.34), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"double\" }, \"payload\": 12.34 }".getBytes()));
    }


    @Test
    public void bytesToConnect() throws UnsupportedEncodingException {
        ByteBuffer reference = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\" }, \"payload\": \"dGVzdC1zdHJpbmc=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        ByteBuffer converted = ByteBuffer.wrap((byte[]) schemaAndValue.value());
        assertEquals(reference, converted);
    }

    @Test
    public void stringToConnect() {
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "foo-bar-baz"), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"string\" }, \"payload\": \"foo-bar-baz\" }".getBytes()));
    }

    @Test
    public void arrayToConnect() {
        byte[] arrayJson = "{ \"schema\": { \"type\": \"array\", \"items\": { \"type\" : \"int32\" } }, \"payload\": [1, 2, 3] }".getBytes();
        assertEquals(new SchemaAndValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList(1, 2, 3)), converter.toConnectData(TOPIC, arrayJson));
    }

    @Test
    public void mapToConnectStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"string\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": { \"key1\": 12, \"key2\": 15} }".getBytes();
        Map<String, Integer> expected = new HashMap<>();
        expected.put("key1", 12);
        expected.put("key2", 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toConnectData(TOPIC, mapJson));
    }

    @Test
    public void mapToConnectNonStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"int32\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": [ [1, 12], [2, 15] ] }".getBytes();
        Map<Integer, Integer> expected = new HashMap<>();
        expected.put(1, 12);
        expected.put(2, 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toConnectData(TOPIC, mapJson));
    }

    @Test
    public void structToConnect() {
        byte[] structJson = "{ \"schema\": { \"type\": \"struct\", \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\" }, { \"field\": \"field2\", \"type\": \"string\" }] }, \"payload\": { \"field1\": true, \"field2\": \"string\" } }".getBytes();
        Schema expectedSchema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string");
        SchemaAndValue converted = converter.toConnectData(TOPIC, structJson);
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test
    public void nullToConnect() {
        // When schemas are enabled, trying to decode a tombstone should be an empty envelope
        // the behavior is the same as when the json is "{ "schema": null, "payload": null }"
        // to keep compatibility with the record
        SchemaAndValue converted = converter.toConnectData(TOPIC, null);
        assertEquals(SchemaAndValue.NULL, converted);
    }

    @Test
    public void nullSchemaPrimitiveToConnect() {
        SchemaAndValue converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": null }".getBytes());
        assertEquals(SchemaAndValue.NULL, converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": true }".getBytes());
        assertEquals(new SchemaAndValue(null, true), converted);

        // Integers: Connect has more data types, and JSON unfortunately mixes all number types. We try to preserve
        // info as best we can, so we always use the largest integer and floating point numbers we can and have Jackson
        // determine if it's an integer or not
        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12L), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12.24 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12.24), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": \"a string\" }".getBytes());
        assertEquals(new SchemaAndValue(null, "a string"), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": [1, \"2\", 3] }".getBytes());
        assertEquals(new SchemaAndValue(null, Arrays.asList(1L, "2", 3L)), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": { \"field1\": 1, \"field2\": 2} }".getBytes());
        Map<String, Long> obj = new HashMap<>();
        obj.put("field1", 1L);
        obj.put("field2", 2L);
        assertEquals(new SchemaAndValue(null, obj), converted);
    }

    @Test
    public void decimalToConnect() {
        Schema schema = Decimal.schema(2);
        BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        // Payload is base64 encoded byte[]{0, -100}, which is the two's complement encoding of 156.
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }, \"payload\": \"AJw=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        BigDecimal converted = (BigDecimal) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void decimalToConnectOptional() {
        Schema schema = Decimal.builder(2).optional().schema();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"optional\": true, \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void decimalToConnectWithDefaultValue() {
        BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        Schema schema = Decimal.builder(2).defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"default\": \"AJw=\", \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void decimalToConnectOptionalWithDefaultValue() {
        BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        Schema schema = Decimal.builder(2).optional().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"optional\": true, \"default\": \"AJw=\", \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void dateToConnect() {
        Schema schema = Date.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 10000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1 }, \"payload\": 10000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void dateToConnectOptional() {
        Schema schema = Date.builder().optional().schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void dateToConnectWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Date.builder().defaultValue(reference).schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void dateToConnectOptionalWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Date.builder().optional().defaultValue(reference).schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1, \"optional\": true, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timeToConnect() {
        Schema schema = Time.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 14400000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1 }, \"payload\": 14400000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timeToConnectOptional() {
        Schema schema = Time.builder().optional().schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void timeToConnectWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Time.builder().defaultValue(reference).schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timeToConnectOptionalWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Time.builder().optional().defaultValue(reference).schema();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1, \"optional\": true, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timestampToConnect() {
        Schema schema = Timestamp.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 2000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1 }, \"payload\": 4000000000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timestampToConnectOptional() {
        Schema schema = Timestamp.builder().optional().schema();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void timestampToConnectWithDefaultValue() {
        Schema schema = Timestamp.builder().defaultValue(new java.util.Date(42)).schema();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1, \"default\": 42 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(new java.util.Date(42), schemaAndValue.value());
    }

    @Test
    public void timestampToConnectOptionalWithDefaultValue() {
        Schema schema = Timestamp.builder().optional().defaultValue(new java.util.Date(42)).schema();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1,  \"optional\": true, \"default\": 42 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(new java.util.Date(42), schemaAndValue.value());
    }

    @Test
    public void structSchemaIdentical() {
        Schema schema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .field("field4", Schema.BOOLEAN_SCHEMA).build();
        Schema inputSchema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .field("field3", Schema.STRING_SCHEMA)
                .field("field4", Schema.BOOLEAN_SCHEMA).build();
        Struct input = new Struct(inputSchema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false);
        assertStructSchemaEqual(schema, input);
    }

    @Test
    public void nullSchemaAndNullValueToJson() {
        // This characterizes the production of tombstone messages when Json schemas is enabled
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", true);
        converter.configure(props, true);
        byte[] converted = converter.fromConnectData(TOPIC, null, null);
        assertNull(converted);
    }

    @Test
    public void nullValueToJson() {
        // This characterizes the production of tombstone messages when Json schemas is not enabled
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props, true);
        byte[] converted = converter.fromConnectData(TOPIC, null, null);
        assertNull(converted);
    }

    @Test(expected = DataException.class)
    public void mismatchSchemaJson() {
        // If we have mismatching schema info, we should properly convert to a DataException
        converter.fromConnectData(TOPIC, Schema.FLOAT64_SCHEMA, true);
    }

    @Test
    public void noSchemaToConnect() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props, true);
        assertEquals(new SchemaAndValue(null, true), converter.toConnectData(TOPIC, "true".getBytes()));
    }

    @Test
    public void noSchemaToJson() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props, true);
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, true));
        assertTrue(converted.isBoolean());
        assertEquals(true, converted.booleanValue());
    }

    // Note: the header conversion methods delegates to the data conversion methods, which are tested above.
    // The following simply verify that the delegation works.

    @Test
    public void stringHeaderToConnect() {
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "foo-bar-baz"), converter.toConnectHeader(TOPIC, "headerName", "{ \"schema\": { \"type\": \"string\" }, \"payload\": \"foo-bar-baz\" }".getBytes()));
    }


    private JsonNode parse(byte[] json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }

    private void assertStructSchemaEqual(Schema schema, Struct struct) {
        converter.fromConnectData(TOPIC, schema, struct);
        assertEquals(schema, struct.schema());
    }
}
