package com.example.bigdata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

// obsolete? - idk if will be used
public class MyEventTimeExtractor implements TimestampExtractor {

    public long extract(final ConsumerRecord<Object, Object> record,
                        final long previousTimestamp) {
        long timestamp = -1;
        String stringLine;

        if (record.value() instanceof String) {
            stringLine = (String) record.value();
            if (StockDataRecord.lineIsCorrect(stringLine)) {
                timestamp = StockDataRecord.parseFromLogLine(stringLine).
                        getTimestampInMillis();
            }
        }
        return timestamp;
    }
}