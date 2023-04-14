cd to /code/env/

docker-compose up -d

open two terminals of spark master

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 streamprocessing/tenantstreamapp1.py

and in another:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 streamprocessing/tenantstreamapp2.py


Open 4 kafka broker terminals to see outputs:

kafka-console-consumer.sh --topic tenant1-warn --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic tenant2-warn --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic tenant1-output-data --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic tenant2-output-data --bootstrap-server localhost:9092
