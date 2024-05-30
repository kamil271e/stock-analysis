package com.example.bigdata;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class StockAggregateDeserializer implements Deserializer<StockAggregate> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public StockAggregate deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        double sumClose = buffer.getDouble();
        double minLow = buffer.getDouble();
        double maxHigh = buffer.getDouble();
        double sumVolume = buffer.getDouble();
        int count = buffer.getInt();

        StockAggregate aggregate = new StockAggregate();
        aggregate.setSumClose(sumClose);
        aggregate.setMinLow(minLow);
        aggregate.setMaxHigh(maxHigh);
        aggregate.setSumVolume(sumVolume);
        aggregate.setCount(count);
        return aggregate;
    }

    @Override
    public void close() {
        // No resources to close
    }
}
