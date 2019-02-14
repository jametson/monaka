package com.jametson.monaka.config;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTimeZone;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class HdfsSinkConnectorTestBase {
    protected static final String TOPIC = "topic";
    protected static final int PARTITION = 1;
    protected static final String timeZoneString = TimeZone.getDefault().getID();
    protected static final DateTimeZone DATE_TIME_ZONE = DateTimeZone.forID(timeZoneString);

    protected Schema createSchemaWithTimeField() {
        return SchemaBuilder.struct().name("record").version(2)
                .field("timestamp", Schema.INT64_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", SchemaBuilder.string().defaultValue("abc").build())
                .build();
    }

    protected Struct createRecordWithTimeField(Schema schema, long timestamp) {
        return new Struct(schema)
                .put("timestamp", timestamp)
                .put("int", 12)
                .put("long", 12L)
                .put("float", 12.2f)
                .put("double", 12.2)
                .put("string", "def");
    }

    protected Map<String, Object> createConfigBasePartition() {
        Map<String, Object> config = new HashMap<>();
        config.put(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
        config.put(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, "YYYY/MM/dd/HH/");
        config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, Locale.ENGLISH.toString());
        config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());

        return config;
    }

    protected Map<String, Object> createConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(HdfsSinkConnectorConfig.PARTITION_DURATION_MS_CONFIG, TimeUnit.HOURS.toMillis(1));
        config.put(HdfsSinkConnectorConfig.LOCALE_CONFIG, Locale.ENGLISH.toString());
        config.put(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, DATE_TIME_ZONE.toString());

        return config;
    }

    protected Map<String, Object> createConfigFieldName(String fieldName) {
        Map<String, Object> config = new HashMap<>();
        config.put(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG, fieldName);

        return config;
    }

    protected SinkRecord createSinkRecord(long timestamp) {
        Schema schema = createSchemaWithTimeField();
        Struct record = createRecordWithTimeField(schema, timestamp);
        return new SinkRecord(TOPIC, PARTITION, Schema.STRING_SCHEMA, null, schema, record, 0L);
    }
}
