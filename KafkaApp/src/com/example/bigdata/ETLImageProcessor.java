package com.example.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Map;

public class ETLImageProcessor {
    public static void process(KStream<String, StockData> stockData, Map<String, String> symbolToNameMap, String aggregationTopic, String aggregationStore) {
        stockData.map((key, value) -> new KeyValue<>(value.getStock() + "-" + value.getDate().getYear() + "-" + value.getDate().getMonthValue(), value))
                .groupByKey(Grouped.with(Serdes.String(), new StockDataSerde()))
                .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
                .aggregate(Aggregation::new, (aggKey, newValue, aggValue) -> aggValue.add(newValue),
                        Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as(aggregationStore)
                                .withKeySerde(Serdes.String()).withValueSerde(new AggregationSerde()))
                .toStream()
                .peek((windowedKey, value) -> {
                    String[] parts = windowedKey.key().split("-");
                    System.out.println(value.toSchema(parts[0], symbolToNameMap.getOrDefault(parts[0], ""), Integer.parseInt(parts[1]), Integer.parseInt(parts[2])));
                })
                .mapValues((windowedKey, value) -> {
                    String[] parts = windowedKey.key().split("-");
                    return value.toSchema(parts[0], symbolToNameMap.getOrDefault(parts[0], ""), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
                })
                .to(aggregationTopic, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));
    }
}
