broker addresses:
	kafka1: 128.199.105.69:9091
	kafka2: 128.199.105.69:9092
	kafka3: 128.199.105.69:9093
connect to one of these is enough to transfer data.

example topic: "demo"

test code on terminal:
	bin/kafka-console-producer.sh --topic demo --bootstrap-server 128.199.105.69:9091
	bin/kafka-console-consumer.sh --topic demo quickstart-events --from-beginning --bootstrap-server 128.199.105.69:9093


