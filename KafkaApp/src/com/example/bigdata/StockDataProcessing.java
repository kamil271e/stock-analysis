package com.example.bigdata;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class StockDataProcessing {

//    public static void main(String[] args) {
//        if (args.length < 4) {
//            System.err.println("Please provide the following parameters: <topic> <D (days)> <P (percentage)> <delay (A or C)>");
//            System.exit(1);
//        }
//
//        String topic = args[0]; // temat Kafki
//        int D = Integer.parseInt(args[1]); // długość okresu czasu w dniach
//        double P = Double.parseDouble(args[2]); // minimalny stosunek różnicy kursów
//        String delay = args[3]; // tryb przetwarzania: A lub C
//
//        System.out.println("Starting StockDataProcessing application with topic: " + topic + ", D: " + D + ", P: " + P + ", delay: " + delay);
//
//        Properties props = new Properties();
//        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-data-processor");
//        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class.getName());
//
//        AggregationSerde aggregationSerde = new AggregationSerde();
//
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> source = builder.stream(topic); // użycie tematu z argumentów wejściowych
//        source.peek((key, value) -> {
//            System.out.println("key:" + key +", value:" + value);
//        });
//
//        // Ładowanie statycznego zbioru danych
//        Map<String, String> symbolMap = StaticDataLoader.loadStaticData();
//        System.out.println("Loaded static data: " + symbolMap.size() + " entries");
//
//        KStream<String, StockData> stockData = source.mapValues(value -> {
//            String[] fields = value.split(",");
//            return new StockData(fields[0], Double.parseDouble(fields[1]), Double.parseDouble(fields[2]), Double.parseDouble(fields[3]), Double.parseDouble(fields[4]), Double.parseDouble(fields[5]), Long.parseLong(fields[6]), fields[7]);
//        });
//
//        // Mapowanie wartości Stock na Security Name
//        KStream<String, StockData> mappedStockData = stockData.mapValues(value -> {
//            String securityName = symbolMap.get(value.getStock());
//            value.setStock(securityName);
//            return value;
//        });
//        mappedStockData.peek((key, value) -> System.out.println("Mapped StockData: key=" + key + ", value=" + value));
//
//
//        // Agregacje w zależności od trybu przetwarzania
//        if (delay.equals("A")) {
//            // Tryb A: najmniejsze możliwe opóźnienie, wielokrotne aktualizowanie wyników
//            mappedStockData
//                    .groupBy((key, value) -> value.getStock())
//                    .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ofDays(1)))
//                    .aggregate(
//                            Aggregation::new,
//                            (aggKey, newValue, aggValue) -> aggValue.add(newValue),
//                            Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as("aggregated-stream-store")
//                                    .withValueSerde(Serdes.serdeFrom(aggregationSerde.serializer(), aggregationSerde.deserializer()))
//                    )
//                    .toStream()
//                    .peek((key, value) -> System.out.println("Aggregated data: " + key + " -> " + value))
//                    .to("aggregated-stock-data", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.serdeFrom(aggregationSerde.serializer(), aggregationSerde.deserializer())));
//        } else if (delay.equals("C")) {
//            // Tryb C: tylko wyniki ostateczne, bez potrzeby późniejszej aktualizacji
//            mappedStockData
//                    .groupBy((key, value) -> value.getStock())
//                    .windowedBy(TimeWindows.of(Duration.ofDays(30)).grace(Duration.ZERO))
//                    .aggregate(
//                            Aggregation::new,
//                            (aggKey, newValue, aggValue) -> {
//                                aggValue = new Aggregation();
//                                return aggValue.add(newValue);
//                            },
//                            Materialized.<String, Aggregation, WindowStore<Bytes, byte[]>>as("final-aggregated-stream-store")
//                                    .withValueSerde(Serdes.serdeFrom(aggregationSerde.serializer(), aggregationSerde.deserializer()))
//                    )
//                    .toStream()
//                    .peek((key, value) -> System.out.println("Final aggregated data: " + key + " -> " + value))
//                    .to("final-aggregated-stock-data", Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.serdeFrom(aggregationSerde.serializer(), aggregationSerde.deserializer())));
//        }
//
//        // Wykrywanie anomalii
//        KStream<String, String> anomalies = mappedStockData
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.ofDays(D)))
//                .aggregate(
//                        Aggregation::new,
//                        (aggKey, newValue, aggValue) -> aggValue.add(newValue),
//                        Materialized.with(Serdes.String(), Serdes.serdeFrom(aggregationSerde.serializer(), aggregationSerde.deserializer()))
//                )
//                .toStream()
//                .filter((windowedKey, aggregation) -> aggregation.isAnomaly(P))
//                .map((windowedKey, aggregation) -> KeyValue.pair(windowedKey.toString(), aggregation.toString()))
//                .peek((key, value) -> System.out.println("Anomaly detected: " + key + " -> " + value));
//
//        anomalies.to("anomalies", Produced.with(Serdes.String(), Serdes.String()));
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), props);
//        streams.start();
//
//        source.foreach((key, value) -> System.out.printf("K: '%s', V: '%s'\n", key, value));
//
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//    }
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

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream(topic);
        source.peek((key, value) -> System.out.println("key:" + key + ", value:" + value))
                .foreach((key, value) -> System.out.println("Consumed message: key=" + key + ", value=" + value));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.err.println("Uncaught exception in thread " + thread.getName() + ": " + throwable.getMessage());
            throwable.printStackTrace();
        });

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
