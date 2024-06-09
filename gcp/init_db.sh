#!/bin/bash

# Directory setup
DATA_DIR="/tmp/datadir"
if [ -d "$DATA_DIR" ]; then
    echo "Directory $DATA_DIR already exists. Cleaning up."
    rm -rf "$DATA_DIR"
fi
mkdir "$DATA_DIR"

# Docker container cleanup
CONTAINER_NAME="mymysql"
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Container $CONTAINER_NAME already exists. Removing it."
    docker rm -f $CONTAINER_NAME
fi


mkdir /tmp/datadir
docker run --name mymysql -v /tmp/datadir:/var/lib/mysql -p 6033:3306 -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:debian

sleep 20 # wait for docker to setup

# create user an db
docker exec -i mymysql mysql -uroot -pmy-secret-pw <<EOF
CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream';
CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8;
GRANT ALL ON streamdb.* TO 'streamuser'@'%';
EOF

# create stockETL table
docker exec -i mymysql mysql -u streamuser -pstream streamdb <<EOF
CREATE TABLE stockETL (
    symbol varchar(20),
    name varchar(150),
    year int,
    month int,
    avgClose float,
    minLow float,
    maxHigh float,
    sumVolume int
);
EOF
  # get mysql-connector and jdbc sink
 wget https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
 sudo cp mysql-connector-j-8.0.33.jar /usr/lib/kafka/libs
 wget https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.7.0/kafka-connect-jdbc-10.7.0.jar
 sudo mkdir /usr/lib/kafka/plugin
 sudo cp kafka-connect-jdbc-10.7.0.jar /usr/lib/kafka/plugin

 # config
 echo 'plugin.path=/usr/lib/kafka/plugin
 bootstrap.servers=localhost:9092
 key.converter=org.apache.kafka.connect.storage.StringConverter
 value.converter=org.apache.kafka.connect.json.JsonConverter
 key.converter.schemas.enable=false
 value.converter.schemas.enable=true
 offset.storage.file.filename=/tmp/connect.offsets
 offset.flush.interval.ms=10000' > connect-standalone.properties

 echo 'connection.url=jdbc:mysql://localhost:6033/streamdb
 connection.user=streamuser
 connection.password=stream
 tasks.max=1
 name=kafka-to-mysql-task
 connector.class=io.confluent.connect.jdbc.JdbcSinkConnector
 topics=aggregated-stock-data
 table.name.format=stockETL' > connect-jdbc-sink.properties

 sudo cp /usr/lib/kafka/config/tools-log4j.properties /usr/lib/kafka/config/connect-log4j.properties

 echo "log4j.logger.org.reflections=ERROR" | sudo tee -a /usr/lib/kafka/config/connect-log4j.properties
