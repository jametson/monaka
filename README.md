# Monaka - Custom Partitioner for Kafka Connect HDFS 

Monaka is custom partitioner format for [Kafka Connect HDFS](https://github.com/confluentinc/kafka-connect-hdfs)

During Kafka Connect HDFS doesn't support specifying time field in TimeBasedPartitioner yet, this project is come up to help anyone who need this feature.

Documentation for Kafka Connect can be found [here](http://docs.confluent.io/current/connect/connect-hdfs/docs/index.html)

## How to use
- Create jar file

    mvn package

- Copy jar file to confluent share java jar folder

    cp monaka.jar $CONFLUENT_HOME/share/java/kafka-connect-hdfs/

- In kafka connect hdfs properties, set partitioner class to monaka class.

    partitioner.class=com.adskom.monaka.TimeBasedPartition

- Add variable KAFKA_OPTS before run kafka connect hdfs. KAFKA_OPTS contains java envoirenment properties to set monaka properties.

    KAFKA_OPTS="-Dpartition.time.field.name=created_at" ./bin/connect-standalone etc/schema-registry/connect-avro-standalone.properties etc/kafka-connect-hdfs/quickstart-hdfs.properties

### Partition class
- **TimeBasedPartition**

    Extends TimeBasedPartitioner class of Kafka Connect Hdfs.

    Option:

      - partition.time.field.name
      
      Name of partitioning date field. If it's not set, the format partition is based on currentTimeMillis.

- **HourlyPartition**

    Hourly partitioner. Extends TimeBasedPartition class

    Option:

      - partition.include.date.name

      type boolean. The default format is "yyyy/MM/dd/HH/". If it sets true, the output format partitions would be "year=yyyy/month=MM/day=dd/hour=HH/".

- **DailyPartition**

    Daily partitioner. Extends TimeBasedPartition class

    Option:

      - partition.include.date.name

      type boolean. The default format is "yyyy/MM/dd/". If it sets true, the output format partitions would be "year=yyyy/month=MM/day=dd/".

- **FieldPartition**

    Format partition base on value of field. Extends FieldPartitioner class of Kafka Connect Hdfs.

    Option:

      - partition.include.field.name

        type boolean. default value is false. Default value partition format is "topic/value_of_field/". If it sets false, partition format would be "field_name=topic/value_of_filed/"

## Development
TBD
