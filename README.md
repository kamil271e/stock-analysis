# stock-analysis
Stock data anomaly detection: Apache Kafka Streams

## KafkaProducer guide

Copy data from bucket to HDFS
```sh
hadoop fs -mkdir -p /user/kamil271e
hadoop fs -mkdir /user/kamil271e/stocks_result
mkdir -p ~/gcs_data/stocks_result
gsutil cp -r gs://pbd-24-kk/stocks_result ~/gcs_data/
hadoop fs -put ~/gcs_data/stocks_result/* /user/kamil271e/stocks_result/
```

List data to be sure we good to go:
```sh
hadoop fs -ls /user/kamil271e/stocks_result
```


Start kafka topic:
```sh
kafka-topics.sh --create \
--bootstrap-server ${CLUSTER_NAME}-w-1:9092 \
--replication-factor 2 --partitions 3 --topic stocks-topic
```

Run Kafka producer (for now it works on local fs; there was some issues with hdfs):
```sh
java -cp "/usr/lib/kafka/libs/*:KafkaCSVProducer.jar" KafkaCSVProducer gcs_data/stocks_result stocks-topic 1
```

Troubleshooting TODO:

I've tried to obtain nodename and port with this commands:

```sh
hdfs getconf -confKey fs.defaultFS
hdfs getconf -confKey dfs.namenode.http-address
```

and then run KafkaProducer using HDFS files:
```sh
java -cp "/usr/lib/kafka/libs/*:KafkaCSVProducer.jar" KafkaCSVProducer hdfs://pbd-cluster-m:9870/user/kamil271e/stocks_result stocks-topic 1
```
but files were not loaded properly:C