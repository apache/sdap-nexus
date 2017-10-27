# tcp-shell

This is a [Spring-XD Module](http://docs.spring.io/spring-xd/docs/current/reference/html/#modules) that starts a shell process and then tries to make a TCP connection to it.

The project can be built by running

`./gradlew clean build`

The module can then be uploaded to Spring XD by running the following command in an XD Shell

`module upload --type processor --name tcpshell --file tcp-shell/build/libs/tcpshell-VERSION.jar`