# stock-analysis
Stock data anomaly detection: Apache Kafka Streams

## KafkaProducer

#### Copy data from bucket to local file system
```sh
mkdir -p ~/gcs_data/stocks_result
gsutil cp -r gs://pbd-24-kk/stocks_result ~/gcs_data/
```

#### Run KafkaCSVProducer
```sh
java -cp "/usr/lib/kafka/libs/*:KafkaCSVProducer.jar" KafkaCSVProducer gcs_data/stocks_result stock-records 1
```

## KafkaStreams analyzer:
```
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:KafkaStocks.jar \
com.example.bigdata.StockRecordToAnomaly ${CLUSTER_NAME}-w-0:9092
```

**TODO**: Connect producer and analyzer based on: [link](https://jankiewicz.pl/studenci/bigdata/BS05_w1_23-Kafka-Streams-gcp-zadania.pdf)
