package com.example.bigdata;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class StockDataSerde implements Serde<StockData> {

    @Override
    public Serializer<StockData> serializer() {
        return (topic, data) -> data.toString().getBytes();
    }

    @Override
    public Deserializer<StockData> deserializer() {
        return (topic, data) -> {
            String[] parts = new String(data).split(";");
            if (parts.length != 8) {
                throw new IllegalArgumentException("Invalid data format");
            }

            double[] values = new double[6];
            for (int i = 1; i <= 6; i++) values[i - 1] = Double.parseDouble(parts[i].trim());
            String date = parts[0].trim(), stock = parts[7].trim();

            return new StockData(date, values[0], values[1], values[2], values[3], values[4], values[5], stock);
        };
    }
}
