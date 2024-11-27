### Generate certs

```
sh ./create-certs.sh
```

### test kafka1, zoo with ssl enable is valid

```
// produce
    docker exec --interactive --tty kafka1 kafka-console-producer --bootstrap-server localhost:9092 --topic test-topic --producer.config /etc/kafka/properties/producer.properties

// consume
docker exec --interactive --tty kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic test-topic --from-beginning --consumer.config /etc/kafka/properties/consumer.properties

docker exec --interactive --tty kafka1 kafka-console-consumer --bootstrap-server localhost:9092 --topic library-events --from-beginning --consumer.config /etc/kafka/properties/consumer.properties

```
