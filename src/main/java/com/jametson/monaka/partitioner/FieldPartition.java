package com.jametson.monaka.partitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;
import io.confluent.connect.hdfs.partitioner.FieldPartitioner;

public class FieldPartition extends FieldPartitioner{
	private static final Logger log = LoggerFactory.getLogger(FieldPartitioner.class);
	private static String fieldName;
	private List<FieldSchema> partitionFields = new ArrayList<>();
	
	@Override
	public void configure(Map<String, Object> config) {
		fieldName = (String) config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);
		partitionFields.add(new FieldSchema(fieldName, TypeInfoFactory.stringTypeInfo.toString(), ""));
	}
	
	@Override
	public String encodePartition(SinkRecord sinkRecord) {
		Object value = sinkRecord.value();
		Schema valueSchema = sinkRecord.valueSchema();
		
		boolean includeFieldName = System.getProperty("partition.include.field.name", "false").toLowerCase().equals("true");
		
		if (value instanceof Struct) {
			Struct struct = (Struct) value;
			Object partitionKey = struct.get(fieldName);
			Type type = valueSchema.field(fieldName).schema().type();
			
			String pathFormat = "";
			if (includeFieldName) {
				pathFormat = fieldName + "=";
			}
			
			switch (type) {
			case INT8:
			case INT16:
			case INT32:
			case INT64:
				Number record = (Number) partitionKey;
				return pathFormat + record.toString();
			case STRING:
				return pathFormat + (String) partitionKey;
			case BOOLEAN:
				boolean booleanRecord = (boolean) partitionKey;
				return pathFormat + Boolean.toString(booleanRecord);
			default:
				log.error("Type {} is not supported as a partition key.", type.getName());
				throw new PartitionException("Error encoding partition.");
			}
		} else {
			log.error("Value is not Struct type.");
			throw new PartitionException("Error encoding partition.");
		}
	}
	
	@Override
	public List<FieldSchema> partitionFields() {
		return partitionFields;
	}
}
