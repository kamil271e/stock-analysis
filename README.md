# stock-analysis
Stock data anomaly detection: Apache Kafka Streams

## GCP setup and running
**Most of files that should be uploaded to cluster in further steps are included in ```/gcp```.** <br> <br>
Step one is to download ``stocks_result`` data from [link](http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project). 

Then create GCP cluster:
```sh
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-2 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components DOCKER,ZOOKEEPER \
--project ${PROJECT_ID} --max-age=3h \
--metadata "run-on-master=true" \
--initialization-actions \
gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```
And store downloaded data in your bucket. For next instructions you need to open cluster shell.

### Copy data from bucket to local file system
```sh
hadoop fs -copyToLocal gs://<YOUR_BUCKET_NAME>/stocks_result ~/data
```

### Initialize all topics / delete them if any exists
Upload all shell files from ``/gcp`` and chmod them to be executable, then run ``init_topics.sh``:
```sh
chmod +x init_topics.sh
chmod +x delete_topics.sh
chmod +x init_db.sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
./init_topics.sh ${CLUSTER_NAME}-w-0:9092
```

To delete all topics simply run
```sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
./delete_topics.sh ${CLUSTER_NAME}-w-0:9092
```

### Setup consumers
Open **two more** cloud shells, one will be consuming anomaly events and second one ETL image. <br> <br>
First shell (Anomaly):
```sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic anomaly-stock-data 
```
Second shell (ETL): (this is optional, ETL image will be store in mysql databae later)
```sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
kafka-console-consumer.sh --bootstrap-server ${CLUSTER_NAME}-w-0:9092 --topic aggregated-stock-data
```
### Running
#### Main application
Firstly upload ``symbols_valid_meta.csv`` that can be downloaded from [link](http://www.cs.put.poznan.pl/kjankiewicz/bigdata/stream_project). Then put it into static directory:
```sh
mkdir static
mv symbols_valid_meta.csv static/symbols_valid_meta.csv
```

Open yet another **two** cloud shells and upload both ```app.jar``` and ```producer.jar```. <br> <br>
First shell, run main app:
```sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:app.jar com.example.bigdata.StockDataProcessing read-stock-data <D> <P> <delay> ${CLUSTER_NAME}-w-0:9092
```
* If you want to get a lot of results use these params: ``D=10, P=10, delay=A``
* If you want to have just a few anomalies use: ```D=10, P=55, delay=A``` (Feel free to play with those)

Second shell run Kafka producer:
```sh
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
java -cp /usr/lib/kafka/libs/*:producer.jar KafkaCSVProducer data read-stock-data 1
```

### Connector for ETL
Init connection:
```
sudo ./init_db.sh
```

If getting this alert:
```
docker: Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?.
```
Set the passwd and start docker deamon:
```
sudo passwd
systemctl start docker
```
Run sink connector (after ``./init_db`` ends)
```sh
/usr/lib/kafka/bin/connect-standalone.sh connect-standalone.properties connect-jdbc-sink.properties
```

To get data from db run following query:
```sh
docker exec -i mymysql mysql -u streamuser -pstream streamdb -e "select * from stockETL;"
```

<!--## Local setup and run 
In order to run this app on your local linux system: download kafka, run zookeper and kafka server
```
wget https://archive.apache.org/dist/kafka/3.1.0/kafka_2.13-3.1.0.tgz # in home directory
cd kafka_2.13-3.1.0/
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Add kafka to PATH
```
export PATH="~/kafka_2.13-3.1.0/bin
```

Delete topics and init new ones
```
cd scripts
chmod +x init_topics.sh
chmod +x delete_topics.sh
./delete_topics.sh localhost:9092
./init_topics.sh localhost:9092
```

Run consumers for ETL and anomaly detection:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic aggregated-stock-data
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic anomaly-stock-data
```

To run producer and main app open ```KafkaProducer``` and ```KafkaApp``` in Intellij.
And run the both setting appropriate input args. <br>
**TODO**: setup instruction--!>

