go to each broker docker-compose.yml:
	sudo docker-compose -f docker-compose.yml up: start containers including zk and kafka, with verbosity.
	sudo docker-compose -f docker-compose.yml up -d: samethings, run in background
	
	sudo docker-compose down: shutdown containers
	
after starting zk and kafka:
	sudo docker exec -it <docker_id> /bin/sh: access the kafka file system to create topics and more.

r	docker ps: to see all docker processes
	
