# kafka with spring producer

### docker compose

zookeeper: zoo1 </br>
brokers: kafka1, kafka2, kafka3 </br>
The 3 broker servers (kafka1, 2, 3) are in the same clusters.

### docker command

```
docker exec --interactive --tty kafka1 kafka-topics --bootstrap-server kafka1:19092 --list

docker exec --interactive --tty kafka1 kafka-topics --bootstrap-server kafka1:19092 --describe --topic <topic_name>

example output:

        Topic: library-events   Partition: 0    Leader: 1       Replicas: 1,3,2 Isr: 1,3,2
        Topic: library-events   Partition: 1    Leader: 2       Replicas: 2,1,3 Isr: 2,1,3
        Topic: library-events   Partition: 2    Leader: 3       Replicas: 3,2,1 Isr: 3,2,1

```
