# create example topic
docker exec -it infrastucture-kafka-1 \
kafka-topics.sh --create \
--bootstrap-server localhost:9092 \
--topic example_topic

# connect to ksql
docker exec -it infrastucture-ksqldb-cli-1 \
ksql http://localhost:8088

