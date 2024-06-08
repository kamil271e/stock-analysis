package com.example.bigdata;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

public class Aggregation {
    private int count;
    private double sumClose;
    private double sumVolume;
    private double avgClose;
    private double minLow;
    private double maxHigh;

    public Aggregation() {
        this.count = 0;
        this.sumClose = 0.0;
        this.sumVolume = 0.0;
        this.avgClose = 0.0;
        this.minLow = Double.MAX_VALUE;
        this.maxHigh = Double.MIN_VALUE;
    }

    public Aggregation add(StockData data) {
        this.count++;
        this.sumClose += data.getClose();
        this.sumVolume += data.getVolume();
        this.avgClose = this.sumClose / this.count;
        this.minLow = Math.min(this.minLow, data.getLow());
        this.maxHigh = Math.max(this.maxHigh, data.getHigh());
        return this;
    }

    // Getters and setters
    public double getSumClose() {
        return sumClose;
    }

    public void setSumClose(double sumClose) {
        this.sumClose = sumClose;
    }

    public double getMinLow() {
        return minLow;
    }

    public void setMinLow(double minLow) {
        this.minLow = minLow;
    }

    public double getMaxHigh() {
        return maxHigh;
    }

    public void setMaxHigh(double maxHigh) {
        this.maxHigh = maxHigh;
    }

    public double getSumVolume() {
        return sumVolume;
    }

    public void setSumVolume(double sumVolume) {
        this.sumVolume = sumVolume;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getAvgClose() {
        return avgClose;
    }

    public void setAvgClose(double avgClose) {
        this.avgClose = avgClose;
    }

    public String toSchema(String symbol, String name, int year, int month) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            Map<String, Object> payload = Map.of(
                    "symbol", symbol,
                    "name", name,
                    "year", year,
                    "month", month,
                    "avgClose", getAvgClose(),
                    "minLow", getMinLow(),
                    "maxHigh", getMaxHigh(),
                    "sumVolume", getSumVolume()
            );

            Map<String, Object> schema = getSchema();

            Map<String, Object> logEntry = Map.of(
                    "schema", schema,
                    "payload", payload
            );

            return objectMapper.writeValueAsString(logEntry);
        } catch (Exception e) {
            throw new RuntimeException("Serialization failed", e);
        }
    }

    private static Map<String, Object> getSchema() {
        List<Map<String, Object>> metadata = List.of(
                Map.of("field", "symbol", "type", "string", "optional", true),
                Map.of("field", "name", "type", "string", "optional", true),
                Map.of("field", "year", "type", "int32", "optional", true),
                Map.of("field", "month", "type", "int32", "optional", true),
                Map.of("field", "avgClose", "type", "float", "optional", true),
                Map.of("field", "minLow", "type", "float", "optional", true),
                Map.of("field", "maxHigh", "type", "float", "optional", true),
                Map.of("field", "sumVolume", "type", "int64", "optional", true)
        );

        Map<String, Object> schema = Map.of(
                "type", "struct",
                "optional", false,
                "version", 1,
                "fields", metadata
        );
        return schema;
    }

}

