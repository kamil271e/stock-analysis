package com.example.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

public class StockDataProcessing {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("[INPUT_TOPIC, D, P, DELAY, SERVER]");
            System.exit(1);
        }
        String AGGREGATION_TOPIC = "aggregated-stock-data";
        String AGGREGATION_STORE = "aggregated-stream-store";
        String APPLICATION_ID = "stock-data-processor";

        String topic = args[0], delay = args[3], bootstrapServers = args[4];
        int D = Integer.parseInt(args[1]);
        double P = Double.parseDouble(args[2]) / 100;

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(topic);
        Map<String, String> symbolToNameMap = StaticDatasetMapper.loadAndMap();

        KStream<String, StockData> stockData = source.mapValues(value -> {
            String[] fields = value.split(",");
            double[] doubleFields = new double[fields.length - 2];
            for (int i = 1; i < fields.length - 1; i++)  doubleFields[i - 1] = Double.parseDouble(fields[i]);
            return new StockData(fields[0], doubleFields[0], doubleFields[1], doubleFields[2],
                    doubleFields[3], doubleFields[4], doubleFields[5], fields[7]);
        }).selectKey((oldKey, value) -> value.getStock());


        // ETL_IMAGE
//        stockData.map((key, value) -> new KeyValue<>(value.getStock() + "-" + value.getDate().getYear() + "-" + value.getDate().getMonthValue(), value))
//                .groupByKey(Grouped.with(Serdes.String(), new StockDataSerde()))
//                .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
//                .aggregate(Aggregation::new, (aggKey, newValue, aggValue) -> aggValue.add(newValue), Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as(AGGREGATION_STORE).withKeySerde(Serdes.String()).withValueSerde(new AggregationSerde()))
//                .toStream()
//                .peek((windowedKey, value) -> {
//                    String[] parts = windowedKey.key().split("-");
//                    System.out.println(value.toSchema(parts[0], symbolToNameMap.getOrDefault(parts[0], ""), Integer.parseInt(parts[1]), Integer.parseInt(parts[2])));
//                })
//                .mapValues((windowedKey, value) -> {
//                    String[] parts = windowedKey.key().split("-");
//                    return value.toSchema(parts[0], symbolToNameMap.getOrDefault(parts[0], ""), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
//                })
//                .to(AGGREGATION_TOPIC, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));


        // ANOMALY DETECTION
        Map<String, List<StockData>> stockDataHistory = new HashMap<>();
        stockData.foreach((key, value) -> {
            String stockSymbol = value.getStock();
            List<StockData> history = stockDataHistory.computeIfAbsent(stockSymbol, k -> new ArrayList<>());
            history.add(value);
            if (history.size() >= D) {
                LocalDate endDate = value.getDate().toLocalDate();
                LocalDate startDate = endDate.minusDays(D - 1);
                LocalDate finalStartDate = startDate;
                List<StockData> filteredHistory = history.stream()
                        .filter(data -> data.getStock().equals(stockSymbol))
                        .filter(data -> data.getDate().toLocalDate().isAfter(finalStartDate.minusDays(1)) &&
                                data.getDate().toLocalDate().isBefore(endDate.plusDays(1)))
                        .collect(Collectors.toList());

                double maxHigh = filteredHistory.stream().mapToDouble(StockData::getHigh).max().orElse(0);
                double minLow = filteredHistory.stream().mapToDouble(StockData::getLow).min().orElse(0);
                double ratio = (maxHigh - minLow) / maxHigh;

                if (ratio > P) {
                    System.out.println("[ANOMALY]: Stock Symbol: " + stockSymbol +
                            ", Analyzed Period: " + startDate + " to " + endDate +
                            ", Max High: " + maxHigh +
                            ", Min Low: " + minLow +
                            ", Ratio: " + ratio);
                }
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Exception: " + thread.getName() + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
