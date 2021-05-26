package com.jametson.monaka.partitioner;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import org.apache.kafka.common.config.ConfigException;
import org.joda.time.DateTimeZone;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DailyPartition extends TimeBasedPartition {
    private static long partitionDurationMs = TimeUnit.HOURS.toMillis(24);
    private static String pathFormat = "YYYY/MM/dd/";

    @Override
    public void configure(Map<String, Object> config) {
        String localeString = (String) config.get(HdfsSinkConnectorConfig.LOCALE_CONFIG);
        if (localeString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.LOCALE_CONFIG, localeString, "Locale cannot be empty.");
        }

        String timeZoneString = (String) config.get(HdfsSinkConnectorConfig.TIMEZONE_CONFIG);
        if (timeZoneString.equals("")) {
            throw new ConfigException(HdfsSinkConnectorConfig.TIMEZONE_CONFIG, timeZoneString,
                    "Timezone cannot be empty.");
        }

        String hiveIntString = (String) config.get(HdfsSinkConnectorConfig.HIVE_INTEGRATION_CONFIG);
        boolean hiveIntegration = hiveIntString != null && hiveIntString.toLowerCase().equals("true");

        boolean includeDateName = System.getProperty("partition.include.date.name", "false").toLowerCase().equals("true");
        if (includeDateName) {
            pathFormat = "'year'=YYYY/'month'=MM/'day'=dd/";
        }

        Locale locale = new Locale(localeString);
        DateTimeZone timeZone = DateTimeZone.forID(timeZoneString);
        init(partitionDurationMs, pathFormat, locale, timeZone, hiveIntegration);
    }

    public String getPathFormat() {
        return pathFormat;
    }
}
