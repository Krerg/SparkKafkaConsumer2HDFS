**Spark Kafka consumer to HDFS**

**Build**

`gradle jar` to build executable jar file

**Run**

`java -jar (or spark submit) SparkKafkaConsumer2HDFS.jar --hdfs-path 'hdfs_path' --bootstrap-server 'bootstrap-server ip' --kafka-topic 'kafka-topic' --starting-offsets '4 offsets delimited by ,' --batch-size 'batch size'`