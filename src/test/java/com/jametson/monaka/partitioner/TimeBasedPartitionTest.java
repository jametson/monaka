package com.jametson.monaka.partitioner;

import com.jametson.monaka.config.HdfsSinkConnectorTestBase;
import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TimeBasedPartitionTest extends HdfsSinkConnectorTestBase {

    @Test
    public void testGenerateFromTimeField() {
        // set property time field name
        System.setProperty("partition.time.field.name", "timestamp");

        TimeBasedPartition partitioner = new TimeBasedPartition();

        Map<String, Object> config = createConfigBasePartition();
        partitioner.configure(config);

        long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
        SinkRecord sinkRecord = createSinkRecord(timestamp);
        String encodedPartition = partitioner.encodePartition(sinkRecord);

        assertEquals("2016/09/19/16/", encodedPartition);
    }

    @Test
    public void testGenerateWithEmptyTimeField() {
        TimeBasedPartition partitioner = new TimeBasedPartition();

        Map<String, Object> config = createConfigBasePartition();
        partitioner.configure(config);

        long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
        SinkRecord sinkRecord = createSinkRecord(timestamp);
        String encodedPartition = partitioner.encodePartition(sinkRecord);

        DateTimeFormatter fmt = DateTimeFormat.forPattern((String) config.get(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG));
        DateTime now = new DateTime();

        assertEquals(fmt.print(now), encodedPartition);
    }
}
