@See https://blog.rockthejvm.com/zio-kafka/

- `docker-compose up -d` starts Kafka zookeeper and broker
- `docker exec -it broker bash` enters the broker bash
- ```
  kafka-topics \
  --bootstrap-server localhost:9092 \
  --topic updates \
  --create
  ``` 
  creates a new topic named `updates`


kafka-console-producer \
--topic updates \
--broker-list localhost:9092 \
--property parse.key=true \
--property key.separator=,