package com.jametson.monaka.partitioner;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.junit.Test;

import com.jametson.monaka.config.HdfsSinkConnectorTestBase;
import com.jametson.monaka.partitioner.FieldPartition;

public class FieldPartitionTest extends HdfsSinkConnectorTestBase{
	
	@Test
	public void testGenerateWithFieldName() {
		// set property include date name
		System.setProperty("partition.include.field.name", "true");
		
		FieldPartition partitioner = new FieldPartition();
		Map<String, Object> config = createConfigFieldName("timestamp");
		partitioner.configure(config);
		
		long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
		SinkRecord sinkRecord = createSinkRecord(timestamp);
		String encodedPartition = partitioner.encodePartition(sinkRecord);
		
		assertEquals("timestamp=" + timestamp, encodedPartition);
	}
	
	@Test
	public void testGenerateWithoutDateName() {
		FieldPartition partitioner = new FieldPartition();
		Map<String, Object> config = createConfigFieldName("timestamp");
		partitioner.configure(config);
		
		long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
		SinkRecord sinkRecord = createSinkRecord(timestamp);
		String encodedPartition = partitioner.encodePartition(sinkRecord);
		
		assertEquals(String.valueOf(timestamp), encodedPartition);
	}
}
