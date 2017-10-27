#!/bin/bash
 
# NOTE: This requires GNU getopt.  On Mac OS X and FreeBSD, you have to install this
# separately; see below.
TEMP=`getopt -o scah --long singleNode,container,admin,help -n 'nexus-ingest' -- "$@"`

if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

# Note the quotes around `$TEMP': they are essential!
eval set -- "$TEMP"

SINGLENODE=false
CONTAINER=false
ADMIN=false
while true; do
  case "$1" in
    -s | --singleNode ) SINGLENODE=true; shift ;;
    -c | --container ) CONTAINER=true; shift ;;
    -a | --admin ) ADMIN=true; shift ;;
    -h | --help ) 
        echo "usage: nexus-ingest [-s|--singleNode] [-c|--container] [-a|--admin]" >&2
        exit 2
        ;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done

if [ "$SINGLENODE" = true ]; then
    source activate nexus-xd-python-modules
    
    export JAVA_OPTS="-Dgrape.root=/usr/local/repositories/.groovy/grapes -Dgroovy.root=/usr/local/repositories/.groovy/ -Dgrape.config=/usr/local/repositories/.groovy/grapeConfig.xml"
    
    xd-singlenode --hadoopDistro none
elif [ "$CONTAINER"  = true ]; then
    source activate nexus-xd-python-modules
    export SPRING_DATASOURCE_URL="jdbc:mysql://$MYSQL_PORT_3306_TCP_ADDR:$MYSQL_PORT_3306_TCP_PORT/xdjob"
    export SPRING_DATASOURCE_USERNAME=$MYSQL_USER
    export SPRING_DATASOURCE_PASSWORD=$MYSQL_PASSWORD
    export SPRING_DATASOURCE_DRIVERCLASSNAME="com.mysql.jdbc.Driver"

    export ZK_NAMESPACE=$ZOOKEEPER_XD_CHROOT
    export ZK_CLIENT_CONNECT=$ZOOKEEPER_CONNECT
    export ZK_CLIENT_SESSIONTIMEOUT=60000
    export ZK_CLIENT_CONNECTIONTIMEOUT=30000
    export ZK_CLIENT_INITIALRETRYWAIT=1000
    export ZK_CLIENT_RETRYMAXATTEMPTS=3

    export SPRING_REDIS_HOST=$REDIS_ADDR
    export SPRING_REDIS_PORT=$REDIS_PORT
    
    export XD_TRANSPORT="kafka"
    export XD_MESSAGEBUS_KAFKA_BROKERS=$KAFKA_BROKERS
    export XD_MESSAGEBUS_KAFKA_ZKADDRESS=$KAFKA_ZKADDRESS
    export XD_MESSAGEBUS_KAFKA_MODE="embeddedHeaders"
    export XD_MESSAGEBUS_KAFKA_OFFSETMANAGEMENT="kafkaNative"
    export XD_MESSAGEBUS_KAFKA_HEADERS="absolutefilepath,spec"
    export XD_MESSAGEBUS_KAFKA_SOCKETBUFFERSIZE=3097152
    export XD_MESSAGEBUS_KAFKA_DEFAULT_QUEUESIZE=4
    export XD_MESSAGEBUS_KAFKA_DEFAULT_FETCHSIZE=2048576
    
    export JAVA_OPTS="-Dgrape.root=/usr/local/repositories/.groovy/grapes -Dgroovy.root=/usr/local/repositories/.groovy/ -Dgrape.config=/usr/local/repositories/.groovy/grapeConfig.xml"
    
    until nc --send-only -v -w30 $MYSQL_PORT_3306_TCP_ADDR $MYSQL_PORT_3306_TCP_PORT </dev/null
    do
      echo "Waiting for database connection..."
      # wait for 5 seconds before check again
      sleep 5
    done
    
    xd-container --hadoopDistro none
elif [ "$ADMIN"  = true ]; then
    source activate nexus-xd-python-modules
    export SPRING_DATASOURCE_URL="jdbc:mysql://$MYSQL_PORT_3306_TCP_ADDR:$MYSQL_PORT_3306_TCP_PORT/xdjob"
    export SPRING_DATASOURCE_USERNAME=$MYSQL_USER
    export SPRING_DATASOURCE_PASSWORD=$MYSQL_PASSWORD
    export SPRING_DATASOURCE_DRIVERCLASSNAME="com.mysql.jdbc.Driver"

    export ZK_NAMESPACE=$ZOOKEEPER_XD_CHROOT
    export ZK_CLIENT_CONNECT=$ZOOKEEPER_CONNECT
    export ZK_CLIENT_SESSIONTIMEOUT=60000
    export ZK_CLIENT_CONNECTIONTIMEOUT=30000
    export ZK_CLIENT_INITIALRETRYWAIT=1000
    export ZK_CLIENT_RETRYMAXATTEMPTS=3

    export SPRING_REDIS_HOST=$REDIS_ADDR
    export SPRING_REDIS_PORT=$REDIS_PORT
    
    export XD_TRANSPORT="kafka"
    export XD_MESSAGEBUS_KAFKA_BROKERS=$KAFKA_BROKERS
    export XD_MESSAGEBUS_KAFKA_ZKADDRESS=$KAFKA_ZKADDRESS
    export XD_MESSAGEBUS_KAFKA_MODE="embeddedHeaders"
    export XD_MESSAGEBUS_KAFKA_OFFSETMANAGEMENT="kafkaNative"
    export XD_MESSAGEBUS_KAFKA_HEADERS="absolutefilepath,spec"
    export XD_MESSAGEBUS_KAFKA_SOCKETBUFFERSIZE=3097152
    export XD_MESSAGEBUS_KAFKA_DEFAULT_QUEUESIZE=4
    export XD_MESSAGEBUS_KAFKA_DEFAULT_FETCHSIZE=2048576
    
    export JAVA_OPTS="-Dgrape.root=/usr/local/repositories/.groovy/grapes -Dgroovy.root=/usr/local/repositories/.groovy/ -Dgrape.config=/usr/local/repositories/.groovy/grapeConfig.xml"
    
    until nc --send-only -v -w30 $MYSQL_PORT_3306_TCP_ADDR $MYSQL_PORT_3306_TCP_PORT </dev/null
    do
      echo "Waiting for database connection..."
      # wait for 5 seconds before check again
      sleep 5
    done
    
    zookeeper-client -server $ZK_CLIENT_CONNECT -cmd create /$ZOOKEEPER_XD_CHROOT ""
    
    xd-admin --hadoopDistro none
else
    echo "One of -s, -c, or -a is required."
    echo "usage: nexus-ingest [-s|--singleNode] [-c|--container] [-a|--admin]" >&2
    exit 3
fi