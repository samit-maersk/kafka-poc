# Kafka-poc

This repo will give you an idea around producer, consumer, and configuration around zookeeper, kafka, schema-registry, kafka-connect, kafka-rest-proxy and etc..

- [Docker Configuration Parameters for Confluent Platform](https://docs.confluent.io/platform/current/installation/docker/config-reference.html)
- 
- How to get started :
start the docker-compose
```sh
docker compose up -d
```
check the status and see If all the expected components are up and running

```sh
docker compose ps
```
you should see 

```
zookeeper
kafka
schema-registry
```
To create a quick topic , to be used by the POC consumer and receiver :

```sh
# create a topic
docker compose exec -it kafka kafka-topics --create --replication-factor 1 --partitions 1 --topic topic-one --bootstrap-server localhost:9092

# list all the available topics
docker compose exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
``
- start the producer and consumer

