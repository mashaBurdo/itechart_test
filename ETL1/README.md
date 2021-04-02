Create a network if it's not created:

    docker network create --gateway 172.28.0.1 --subnet 172.28.0.0/16 flask_and_etl1

To start docker container execute the following commands:

    docker-compose build
    docker-compose up

Run the following command to see running containers and their ID:

    docker ps

Run the following command move to bash of docker container:

    docker exec -it <CONTAINER ID> bash

Run th following command to create index and transfer there data from sqlite to elasticsearch:

    python ETL_mechanism.py

