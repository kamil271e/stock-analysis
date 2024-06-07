package com.example.bigdata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregationSerde implements Serde<Aggregation> {
    private final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Aggregation> serializer() {
        return new Serializer<Aggregation>() {
            @Override
            public byte[] serialize(String topic, Aggregation data) {
                try {
                    // Construct the JSON payload
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("sumClose", data.getSumClose());
                    payload.put("minLow", data.getMinLow());
                    payload.put("maxHigh", data.getMaxHigh());
                    payload.put("sumVolume", data.getSumVolume());
                    payload.put("avgClose", data.getAvgClose());
                    payload.put("count", data.getCount());

                    // Construct the log entry
                    Map<String, Object> logEntry = new LinkedHashMap<>();
                    logEntry.put("schema", Map.of(
                            "type", "struct",
                            "optional", false,
                            "version", 1,
                            "fields", List.of(
                                    Map.of("field", "sumClose", "type", "double", "optional", true),
                                    Map.of("field", "minLow", "type", "double", "optional", true),
                                    Map.of("field", "maxHigh", "type", "double", "optional", true),
                                    Map.of("field", "sumVolume", "type", "double", "optional", true),
                                    Map.of("field", "avgClose", "type", "double", "optional", true),
                                    Map.of("field", "count", "type", "int32", "optional", true)
                            )
                    ));
                    logEntry.put("payload", payload);

                    // Serialize the log entry to JSON
                    return objectMapper.writeValueAsBytes(logEntry);
                } catch (Exception e) {
                    throw new RuntimeException("Serialization failed", e);
                }
            }
        };
    }


    @Override
    public Deserializer<Aggregation> deserializer() {
        return new Deserializer<Aggregation>() {
            @Override
            public Aggregation deserialize(String topic, byte[] data) {
                try {
                    // Deserialize the JSON payload
                    Map<String, Object> logEntry = objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {});
                    Map<String, Object> payload = (Map<String, Object>) logEntry.get("payload");

                    // Extract fields from the payload
                    String stockSymbol = (String) payload.get("stockSymbol");
                    String stockName = (String) payload.get("stockName");
                    int year = (int) payload.get("year");
                    int month = (int) payload.get("month");
                    double avgClose = (double) payload.get("avgClose");
                    double minLow = (double) payload.get("minLow");
                    double maxHigh = (double) payload.get("maxHigh");
                    double sumVolume = (double) payload.get("sumVolume");

                    // Create an Aggregation object
                    Aggregation aggregation = new Aggregation();
                    // Set the extracted fields
                    aggregation.setStockSymbol(stockSymbol);
                    aggregation.setStockName(stockName);
                    aggregation.setYear(year);
                    aggregation.setMonth(month);
                    aggregation.setAvgClose(avgClose);
                    aggregation.setMinLow(minLow);
                    aggregation.setMaxHigh(maxHigh);
                    aggregation.setSumVolume(sumVolume);

                    return aggregation;
                } catch (Exception e) {
                    throw new RuntimeException("Deserialization failed", e);
                }
            }
        };
    }


}
