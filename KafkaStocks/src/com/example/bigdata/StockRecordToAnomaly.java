package com.example.bigdata;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StockRecordToAnomaly {

    public static void main(String[] args) throws Exception {
        // Parameters for anomaly detection
        // final int D = Integer.parseInt(args[1]); // window duration in days
        // final double P = Double.parseDouble(args[2]); // percentage threshold

        final int D = 2;
        final double P = 30;

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[0]);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "alert-requests-application");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        final Serde<String> stringSerde = Serdes.String();
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream("stock-records", Consumed.with(stringSerde, stringSerde));

        KStream<String, StockDataRecord> stockStream = textLines
                .filter((key, value) -> StockDataRecord.lineIsCorrect(value))
                .mapValues(StockDataRecord::parseFromLogLine);

        // Aggregate information on a monthly basis
        KTable<Windowed<String>, StockAggregate> monthlyAggregates = stockStream
                .groupBy((key, value) -> value.getStock() + "_" + value.getDate().substring(0, 7), Grouped.with(stringSerde, Serdes.serdeFrom(new StockDataRecordSerializer(), new StockDataRecordDeserializer())))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofDays(30)))
                .aggregate(
                        StockAggregate::new,
                        (key, value, aggregate) -> aggregate.add(value),
                        Materialized.<String, StockAggregate, WindowStore<Bytes, byte[]>>as("monthly-aggregates-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(Serdes.serdeFrom(new StockAggregateSerializer(), new StockAggregateDeserializer()))
                );

        // Detect anomalies
        stockStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofDays(D)))
                .aggregate(
                        StockAggregate::new,
                        (key, value, aggregate) -> aggregate.add(value),
                        Materialized.<String, StockAggregate, WindowStore<Bytes, byte[]>>as("anomaly-aggregates-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(Serdes.serdeFrom(new StockAggregateSerializer(), new StockAggregateDeserializer()))
                )
                .toStream()
                .filter((windowedKey, aggregate) -> aggregate.isAnomaly(P))
                .map((windowedKey, aggregate) -> KeyValue.pair(
                        windowedKey.key(),
                        String.format("Stock: %s, Period: %s to %s, Max Price: %.2f, Min Price: %.2f, Ratio: %.2f",
                                windowedKey.key(),
                                windowedKey.window().startTime(),
                                windowedKey.window().endTime(),
                                aggregate.getMaxPrice(),
                                aggregate.getMinPrice(),
                                aggregate.getPriceFluctuationRatio())))
                .to("alert-requests", Produced.with(stringSerde, stringSerde));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
