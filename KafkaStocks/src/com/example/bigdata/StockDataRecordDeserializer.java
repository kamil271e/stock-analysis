package com.example.bigdata;

import com.example.bigdata.StockDataRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;

import java.nio.ByteBuffer;
import java.util.Map;

public class StockDataRecordDeserializer implements Deserializer<StockDataRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration needed
    }

    @Override
    public StockDataRecord deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);

            int size = buffer.getInt();
            byte[] dateBytes = new byte[size];
            buffer.get(dateBytes);
            String date = new String(dateBytes, "UTF-8");

            size = buffer.getInt();
            byte[] openBytes = new byte[size];
            buffer.get(openBytes);
            String open = new String(openBytes, "UTF-8");

            size = buffer.getInt();
            byte[] highBytes = new byte[size];
            buffer.get(highBytes);
            String high = new String(highBytes, "UTF-8");

            size = buffer.getInt();
            byte[] closeBytes = new byte[size];
            buffer.get(closeBytes);
            String close = new String(closeBytes, "UTF-8");

            size = buffer.getInt();
            byte[] adjCloseBytes = new byte[size];
            buffer.get(adjCloseBytes);
            String adjClose = new String(adjCloseBytes, "UTF-8");

            size = buffer.getInt();
            byte[] volumeBytes = new byte[size];
            buffer.get(volumeBytes);
            String volume = new String(volumeBytes, "UTF-8");

            size = buffer.getInt();
            byte[] stockBytes = new byte[size];
            buffer.get(stockBytes);
            String stock = new String(stockBytes, "UTF-8");

            size = buffer.getInt();
            byte[] lowBytes = new byte[size];
            buffer.get(lowBytes);
            String low = new String(stockBytes, "UTF-8");

            return new StockDataRecord(date, open, high, low,  close, adjClose, volume, stock);

        } catch (Exception e) {
            throw new RuntimeException("Error deserializing StockDataRecord", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
