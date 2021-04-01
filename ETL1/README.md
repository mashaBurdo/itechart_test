To start docker container execute the following commands:

    docker-compose build
    docker-compose up

Run the following command to see running containers and their ID:

    docker ps

Run the following command move to bash of docker container:

    docker exec -it <CONTAINER ID> bash

Run th following command to create index and transfer there data from sqlite to elasticsearch:

    python ETL_mechanism.py

