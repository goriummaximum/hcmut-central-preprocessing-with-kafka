go to each broker docker-compose.yml:
	sudo docker-compose -f docker-compose.yml up: start containers including zk and kafka, with verbosity.
	sudo docker-compose -f docker-compose.yml up -d: samethings, run in background
	
	sudo docker-compose down: shutdown containers
	
after starting zk and kafka:
	sudo docker exec -it <docker_id> /bin/sh: access the kafka file system to create topics and more.

	docker ps: to see all docker processes
	
	bin/kafka-topics.sh --create --topic preprocessed-humidity --replication-factor 3 --partitions 1 --bootstrap-server 128.199.105.69:9091: create topic preprocessed-humidity
	
	bin/kafka-topics.sh --describe --topic raw-temperature --bootstrap-server 128.199.105.69:9092: describe topic raw-temperature

	
