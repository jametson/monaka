package com.jametson.monaka.partitioner;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.junit.Test;

import com.jametson.monaka.config.HdfsSinkConnectorTestBase;
import com.jametson.monaka.partitioner.HourlyPartition;

public class HourlyPartitionTest extends HdfsSinkConnectorTestBase{
	
	@Test
	public void testGenerateWithDateName() {
		// set property include date name
		System.setProperty("partition.include.date.name", "true");
		
		// set property time field name
		System.setProperty("partition.time.field.name", "timestamp");
		
		HourlyPartition partitioner = new HourlyPartition();
		Map<String, Object> config = createConfig();
		partitioner.configure(config);
		
		long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
		SinkRecord sinkRecord = createSinkRecord(timestamp);
		String encodedPartition = partitioner.encodePartition(sinkRecord);
		
		assertEquals("year=2016/month=09/day=19/hour=16/", encodedPartition);
	}
	
	@Test
	public void testGenerateWithoutDateName() {
		// set property time field name
		System.setProperty("partition.time.field.name", "timestamp");
		
		HourlyPartition partitioner = new HourlyPartition();
		Map<String, Object> config = createConfig();
		partitioner.configure(config);
		
		long timestamp = new DateTime(2016, 9, 19, 16, 0, 0, 0, DATE_TIME_ZONE).getMillis();
		SinkRecord sinkRecord = createSinkRecord(timestamp);
		String encodedPartition = partitioner.encodePartition(sinkRecord);
		
		assertEquals("2016/09/19/16/", encodedPartition);
	}
}
