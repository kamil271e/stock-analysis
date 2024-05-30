package com.example.bigdata;

import com.example.bigdata.StockDataRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

import java.nio.ByteBuffer;
import java.util.Map;

public class StockDataRecordSerializer implements Serializer<StockDataRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public byte[] serialize(String topic, StockDataRecord data) {
        if (data == null) {
            return null;
        }

        byte[] date, open, high, close, adjClose, volume, stock;

        try {
            date = data.getDate().getBytes("UTF-8");
            open = data.getOpen().getBytes("UTF-8");
            high = data.getHigh().getBytes("UTF-8");
            close = data.getClose().getBytes("UTF-8");
            adjClose = data.getAdjClose().getBytes("UTF-8");
            volume = data.getVolume().getBytes("UTF-8");
            stock = data.getStock().getBytes("UTF-8");

            ByteBuffer buffer = ByteBuffer.allocate(4 + date.length + 4 + open.length + 4 + high.length + 4 + close.length + 4 + adjClose.length + 4 + volume.length + 4 + stock.length);

            buffer.putInt(date.length);
            buffer.put(date);

            buffer.putInt(open.length);
            buffer.put(open);

            buffer.putInt(high.length);
            buffer.put(high);

            buffer.putInt(close.length);
            buffer.put(close);

            buffer.putInt(adjClose.length);
            buffer.put(adjClose);

            buffer.putInt(volume.length);
            buffer.put(volume);

            buffer.putInt(stock.length);
            buffer.put(stock);

            return buffer.array();

        } catch (Exception e) {
            throw new RuntimeException("Error serializing StockDataRecord", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}