# Bigdata-streaming-workshop



###  



```commandline
docker-compose build
```

```commandline
docker-compose up -d
```

```commandline
docker-compose ps
```
```
http://localhost:8081/#/overview
```

```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic input_topic
```
```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic object_ctr_topic  
```
```commandline
docker-compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic object_mean_seen_ms_topic  
```
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/flink_part/object_data.py -d  
```
```commandline
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/flink_part/to_cache.py -d  
```
