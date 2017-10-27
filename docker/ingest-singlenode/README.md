# ingest-singlenode Docker

This can be used to start spring-xd in singlenode mode with all nexus modules already installed.

# Singlenode Mode

To start the server in singleNode mode use:

    docker run -it -v ~/data/:/usr/local/data/nexus -p 9393:9393 --name nexus-ingest nexusjpl/ingest-singlenode

This starts a singleNode instance of Spring XD with a data volume mounted to the host machine's home directory to be used for ingestion. It also exposes the Admin UI on port 9393 of the host machine.

You can then connect to the Admin UI with http://localhost:9393/admin-ui.

# XD Shell

## Using Docker Exec

Once the nexus-ingest container is running you can use docker exec to start an XD Shell that communicates with the singlenode server:

    docker exec -it nexus-ingest xd-shell

## Using Standalone Container

You can use the springxd shell docker image to start a seperate container running XD shell connected to the singlenode server:

    docker run -it --network container:nexus-ingest springxd/shell