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
java -cp /usr/lib/kafka/libs/*:StockDataProcessing.jar \
com.example.bigdata.StockDataProcessing stock-records 10 1 A ${CLUSTER_NAME}-w-0:9092
```

**TODO**: Connect producer and analyzer based on: [link](https://jankiewicz.pl/studenci/bigdata/BS05_w1_23-Kafka-Streams-gcp-zadania.pdf)


#### Random notes:

* Create project:

file -> project structure -> libraries -> from maven

paste lib:

```
org.apache.kafka:kafka-clients:3.1.0
org.apache.kafka:kafka-streams:3.1.0
```

* Add inpuy params in intelij:
ALT+SHIFT+F10, Right, Edit

* Generate jar:
file -> project structure -> artifacts -> add empty; add module <br>
build -> build artifacts

stored at out/artifacts

* Topic list:

```
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --list
```
## Local setup and run 
Cheatsheet
```
wget https://archive.apache.org/dist/kafka/3.1.0/kafka_2.13-3.1.0.tgz
cd kafka_2.13-3.1.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
