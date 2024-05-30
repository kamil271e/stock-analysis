package com.example.bigdata;

import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class StockAggregateSerializer implements Serializer<StockAggregate> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, StockAggregate data) {
        if (data == null) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.allocate(8 * 4 + 4);
        buffer.putDouble(data.getAvgClose());
        buffer.putDouble(data.getMinLow());
        buffer.putDouble(data.getMaxHigh());
        buffer.putDouble(data.getSumVolume());
        buffer.putInt(data.getCount());
        return buffer.array();
    }

    @Override
    public void close() {
        // No resources to close
    }
}
