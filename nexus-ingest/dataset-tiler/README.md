# dataset-tiler

[Spring-XD Module](http://docs.spring.io/spring-xd/docs/current/reference/html/#modules) that creates SectionSpecs that can be used to read the data from the dataset in tiles.

The project can be built by running

`./gradlew clean build`

The module can then be uploaded to Spring XD by running the following command in an XD Shell

`module upload --type processor --name dataset-tiler --file dataset-tiler/build/libs/dataset-tiler-VERSION.jar`