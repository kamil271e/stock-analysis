# stock-analysis
Stock data anomaly detection: Apache Kafka Streams

## KafkaProducer

#### Copy data from bucket to HDFS
```sh
hadoop fs -mkdir -p /user/kamil271e
hadoop fs -mkdir /user/kamil271e/stocks_result
mkdir -p ~/gcs_data/stocks_result
gsutil cp -r gs://pbd-24-kk/stocks_result ~/gcs_data/
hadoop fs -put ~/gcs_data/stocks_result/* /user/kamil271e/stocks_result/
```

#### List data to be sure we good to go:
```sh
hadoop fs -ls /user/kamil271e/stocks_result
```

#### Run KafkaCSVProducer
(for now it works on local fs; there was some issues with hdfs):
```sh
java -cp "/usr/lib/kafka/libs/*:KafkaCSVProducer.jar" KafkaCSVProducer gcs_data/stocks_result stock-records 1
```

Troubleshooting on HDFS:
I've tried to obtain nodename and port with this commands:

```sh
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey dfs.namenode.http-address
```

and then run KafkaProducer using HDFS files:
```sh
java -cp "/usr/lib/kafka/libs/*:KafkaCSVProducer.jar" KafkaCSVProducer hdfs://pbd-cluster-m:9870/user/kamil271e/stocks_result stock-records 1
```
but files were not loaded properly:C

## KafkaStreams analyzer:
```
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:KafkaStocks.jar \
com.example.bigdata.StockRecordToAnomaly ${CLUSTER_NAME}-w-0:9092
```
**TODO**: Connect producer and analyzer based on: [link](https://jankiewicz.pl/studenci/bigdata/BS05_w1_23-Kafka-Streams-gcp-zadania.pdf)
