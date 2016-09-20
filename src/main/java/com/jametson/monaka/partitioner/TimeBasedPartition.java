package com.jametson.monaka.partitioner;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.partitioner.TimeBasedPartitioner;

public class TimeBasedPartition extends TimeBasedPartitioner {

	// Duration of a partition in milliseconds.
	private long partitionDurationMs;
	private DateTimeFormatter formatter;
	private String timeFieldName;
	private static String patternString = "'year'=Y{1,5}/('month'=M{1,5}/)?('day'=d{1,3}/)?('hour'=H{1,3}/)?('minute'=m{1,3}/)?";
	private static Pattern pattern = Pattern.compile(patternString);

	@Override
	protected void init(long partitionDurationMs, String pathFormat, Locale locale, DateTimeZone timeZone,
			boolean hiveIntegration) {
		this.partitionDurationMs = partitionDurationMs;
		this.formatter = getDateTimeFormatter(pathFormat, timeZone).withLocale(locale);
		this.timeFieldName = System.getProperty("partition.time.field.name", "");
		addToPartitionFields(pathFormat, hiveIntegration);
	}
	
	private static DateTimeFormatter getDateTimeFormatter(String str, DateTimeZone timeZone) {
		return DateTimeFormat.forPattern(str).withZone(timeZone);
	}

	@Override
	public String encodePartition(SinkRecord sinkRecord) {
		long timestamp;
		
		if (timeFieldName.equals("")) {
			timestamp = System.currentTimeMillis();
		} else {
			try {
				Struct struct = (Struct) sinkRecord.value();
				timestamp = (long) struct.get(timeFieldName);
			} catch (ClassCastException | DataException e) {
				throw new ConnectException(e);
			}
		}

		DateTime bucket = new DateTime(getPartition(partitionDurationMs, timestamp, formatter.getZone()));
		return bucket.toString(formatter);
	}
	
	private boolean verifyDateTimeFormat(String pathFormat) {
		Matcher m = pattern.matcher(pathFormat);
		return m.matches();
	}
	
	private void addToPartitionFields(String pathFormat, boolean hiveIntegration) {
		if (hiveIntegration && !verifyDateTimeFormat(pathFormat)) {
			throw new ConfigException(HdfsSinkConnectorConfig.PATH_FORMAT_CONFIG, pathFormat,
					"Path format doesn't meet the requirements for Hive integration, "
							+ "which require prefixing each DateTime component with its name.");
		}
		
		for (String field : pathFormat.split("/")) {
			String[] parts = field.split("=");
			FieldSchema fieldSchema = new FieldSchema(parts[0].replace("'", ""),
					TypeInfoFactory.stringTypeInfo.toString(), "");
			partitionFields.add(fieldSchema);
		}
	}
}
