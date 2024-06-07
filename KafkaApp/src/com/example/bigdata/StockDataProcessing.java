package com.example.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class StockDataProcessing {
    public static void main(String[] args) {
        if (args.length < 5) {
            System.err.println("Please provide the following parameters: <topic> <D (days)> <P (percentage)> <delay (A or C)> <bootstrap-servers>");
            System.exit(1);
        }

        String topic = args[0]; // temat Kafki
        int D = Integer.parseInt(args[1]); // długość okresu czasu w dniach
        double P = Double.parseDouble(args[2]); // minimalny stosunek różnicy kursów
        String delay = args[3]; // tryb przetwarzania: A lub C
        String bootstrapServers = args[4]; // Kafka bootstrap servers

        System.out.println("Starting StockDataProcessing application with topic: " + topic + ", D: " + D + ", P: " + P + ", delay: " + delay);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-data-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        AggregationSerde aggregationSerde = new AggregationSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(topic);
        source.peek((key, value) -> System.out.println())
                .foreach((key, value) -> System.out.println("[START]: key=" + key + ", value=" + value));

        Map<String, String> symbolMap = StaticDataLoader.loadStaticData();
        System.out.println("Loaded static data: " + symbolMap.size() + " entries");

        KStream<String, StockData> stockData = source.mapValues(value -> {
            String[] fields = value.split(",");
            return new StockData(fields[0],
                    Double.parseDouble(fields[1]),
                    Double.parseDouble(fields[2]),
                    Double.parseDouble(fields[3]),
                    Double.parseDouble(fields[4]),
                    Double.parseDouble(fields[5]),
                    Double.parseDouble(fields[6]),
                    fields[7]);
        }).selectKey((oldKey, value) -> value.getStock());

        KStream<String, StockData> mappedStockData = stockData.mapValues(value -> {
            String securityName = symbolMap.get(value.getStock());
            value.setStock(securityName);
            return value;
        });
        mappedStockData.peek((key, value) -> System.out.println("[MAPPED]: key=" + key + ", value=" + value));

        if (delay.equals("A")) {
            mappedStockData
                    .groupBy((key, value) -> value.getStock(), Grouped.with(Serdes.String(), new StockDataSerde()))
                    .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
                    .aggregate(
                            Aggregation::new,
                            (aggKey, newValue, aggValue) -> aggValue.add(newValue),
                            Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as("aggregated-stream-store")
                                    .withKeySerde(Serdes.String())
                                    .withValueSerde(new AggregationSerde())
                    )
                    .toStream()
                    .peek((key, value) -> System.out.println("[AGGREGATED] " + key + " -> " + value))
                    .to("aggregated-stock-data", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new AggregationSerde()));
        }


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Uncaught exception in thread " + thread.getName() + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
