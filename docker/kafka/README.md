

This Docker container runs Apache Kafka 2.11-0.9.0.1 on CentOs 7 with Oracle jdk-8u121-linux-x64.

The easiest way to run it is:

    docker run -it --add-host="zkhost:10.200.10.1" -p 9092:9092 nexusjpl/kafka

The default command when running this container is the `kafka-server-start.sh` script using the `/usr/local/kafka/current/config/server.properties` configuration file. 

Be default, the server.properties file is configured to connect to zookeeper as such:

    zookeeper.connect=zkhost:2181/kafka

So by specifying `--add-host="zkhost:10.200.10.1"` with a valid IP address to a zookeeper node, Kafka will be able to connect to an existing cluster.

If you need to override any of the configuration you can use:

    docker run -it --add-host="zkhost:10.200.10.1" nexusjpl/kafka kafka-server-start.sh /usr/local/kafka/current/config/server.properties --override property=value