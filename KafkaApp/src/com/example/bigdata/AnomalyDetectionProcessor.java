package com.example.bigdata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.kstream.KStream;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class AnomalyDetectionProcessor {
    public static void process(KStream<String, StockData> stockData, int D, double P, String anomalyTopic) {
        KafkaProducer<String, String> anomalyProducer = AnomalyProducer.createProducer();
        Map<String, List<StockData>> historyMap = new HashMap<>();
        stockData.foreach((key, value) -> {
            String stockSymbol = value.getStock();
            List<StockData> history = historyMap.computeIfAbsent(stockSymbol, k -> new ArrayList<>());
            history.add(value);
            if (history.size() >= D) {
                LocalDate endDate = value.getDate().toLocalDate();
                LocalDate startDate = endDate.minusDays(D - 1);
                List<StockData> filteredHistory = history.stream()
                        .filter(data -> data.getStock().equals(stockSymbol))
                        .filter(data -> {
                            LocalDate dataDate = data.getDate().toLocalDate();
                            return !dataDate.isBefore(startDate) && !dataDate.isAfter(endDate);
                        }).collect(Collectors.toList());

                double high = filteredHistory.stream().mapToDouble(StockData::getHigh).max().orElse(0);
                double low = filteredHistory.stream().mapToDouble(StockData::getLow).min().orElse(0);
                double ratio = (high - low) / high;

                if (ratio > P) {
                    String anomalyMsg = String.format("[ANOMALY]: %s %s -- %s, H: %f, L: %f, Ratio: %f", stockSymbol, startDate, endDate, high, low, ratio);
                    System.out.println(anomalyMsg);
                    ProducerRecord<String, String> record = new ProducerRecord<>(anomalyTopic, stockSymbol, anomalyMsg);
                    anomalyProducer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            exception.printStackTrace();
                            System.err.println("Error sending message to Kafka: " + exception.getMessage());
                        }
                    });
                }
            }
        });
    }
}
