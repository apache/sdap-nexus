# ingest-admin Docker

This can be used to start a spring-xd admin node.

# Docker Compose

Use the [docker-compose.yml](docker-compose.yml) file to start up mysql, redis, and xd-admin in one command. Example:

    MYSQL_PASSWORD=admin ZK_HOST_IP=10.200.10.1 KAFKA_HOST_IP=10.200.10.1 docker-compose up

`MYSQL_PASSWORD` sets the password for a new MySQL user called `xd` when the MySQL database is initialized.
`ZK_HOST_IP` must be set to a valid IP address of a zookeeper host that will be used to manage Spring XD.
`KAFKA_HOST_IP` must be set to a valid IP address of a kafka broker that will be used for the transport layer of Spring XD

# Docker Run

This container relies on 4 external services that must already be running: MySQL, Redis, Zookeeper, and Kafka.

To start the server use:

    docker run -it \
    -e "MYSQL_PORT_3306_TCP_ADDR=mysqldb" -e "MYSQL_PORT_3306_TCP_PORT=3306" \
    -e "MYSQL_USER=xd" -e "MYSQL_PASSWORD=admin" \
    -e "REDIS_ADDR=redis" -e "REDIS_PORT=6397" \
    -e "ZOOKEEPER_CONNECT=zkhost:2181" -e "ZOOKEEPER_XD_CHROOT=springxd" \
    -e "KAFKA_BROKERS=kafka1:9092" -e "KAFKA_ZKADDRESS=zkhost:2181/kafka"
    --add-host="zkhost:10.200.10.1" \
    --add-host="kafka1:10.200.10.1"
    --name xd-admin nexusjpl/ingest-admin

This mode requires a number of Environment Variables to be defined.

#####  `MYSQL_PORT_3306_TCP_ADDR`

Address to a running MySQL service

#####  `MYSQL_PORT_3306_TCP_PORT`

Port for running MySQL service

#####  `MYSQL_USER`

Username to connnect to MySQL service

#####  `MYSQL_PASSWORD`

Password for connecting to MySQL service

#####  `ZOOKEEPER_CONNECT`

Zookeeper connect string. Can be a comma-delimmited list of host:port values.

#####  `ZOOKEEPER_XD_CHROOT`

Zookeeper root node for spring-xd

#####  `REDIS_ADDR`

Address to a running Redis service

#####  `REDIS_PORT`

Port for running Redis service

#####  `KAFKA_BROKERS`

Comma-delimmited list of host:port values which define the list of Kafka brokers used for transport.

#####  `KAFKA_ZKADDRESS`

Specifies the ZooKeeper connection string in the form hostname:port where host and port are the host and port of a ZooKeeper server.  

The server may also have a ZooKeeper chroot path as part of its ZooKeeper connection string which puts its data under some path in the global ZooKeeper namespace. If so the consumer should use the same chroot path in its connection string. For example to give a chroot path of `/chroot/path` you would give the connection string as `hostname1:port1,hostname2:port2,hostname3:port3/chroot/path`.

# XD Shell

## Using Docker Exec

Once the xd-admin container is running you can use docker exec to start an XD Shell that communicates with the xd-admin server:

    docker exec -it xd-admin xd-shell

## Using Standalone Container
You can use the springxd shell docker image to start a seperate container running XD shell connected to the xd-admin server:

    docker run -it --network container:xd-admin springxd/shell