
# How to Build

All docker builds should happen from this directory. For copy/paste ability, first export the environment variable `BUILD_VERSION` to the version number you would like to tag images as.

## spark-mesos-base

    docker build -t sdap/spark-mesos-base:${BUILD_VERSION} -f mesos/base/Dockerfile .

## spark-mesos-master

Builds from `spark-mesos-base` and supports `tag_version` build argument which specifies the version of base to build from.

    docker build -t sdap/spark-mesos-master:${BUILD_VERSION} -f mesos/master/Dockerfile .

## spark-mesos-agent

Builds from `spark-mesos-base` and supports `tag_version` build argument which specifies the version of base to build from.

    docker build -t sdap/spark-mesos-agent:${BUILD_VERSION} -f mesos/agent/Dockerfile .

## nexus-webapp:mesos

Builds from `spark-mesos-base` and supports `tag_version` build argument which specifies the version of base to build from.

    docker build -t sdap/nexus-webapp:mesos.${BUILD_VERSION} -f mesos/webapp/Dockerfile .

## nexus-webapp:standalone

    docker build -t sdap/nexus-webapp:standalone.${BUILD_VERSION} -f standalone/Dockerfile .

# Push Images

Push the images to the `sdap` organization on DockerHub

    docker push sdap/spark-mesos-base:${BUILD_VERSION}
    docker push sdap/spark-mesos-master:${BUILD_VERSION}
    docker push sdap/spark-mesos-agent:${BUILD_VERSION}
    docker push sdap/nexus-webapp:mesos.${BUILD_VERSION}
    docker push sdap/nexus-webapp:standalone.${BUILD_VERSION}
    
