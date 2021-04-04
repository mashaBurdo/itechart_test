[comment]: <> (Create a network if it's not created:)

[comment]: <> (    docker network create --gateway 172.28.0.1 --subnet 172.28.0.0/16 flask_and_etl1)

To start docker container execute the following commands:

    docker-compose build
    docker-compose up

That will start Elasticsearch on port 9200, create index and store movies.

