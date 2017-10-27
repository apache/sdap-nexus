# nexus-sink

[Spring-XD Module](http://docs.spring.io/spring-xd/docs/current/reference/html/#modules) that handles saving a NexusTile to both Solr and Cassandra.

The project can be built by running

`./gradlew clean build`

The module can then be uploaded to Spring XD by running the following command in an XD Shell

`module upload --type sink --name nexus --file nexus-sink/build/libs/nexus-sink-VERSION.jar`