
# Build the image

To build the nexus-webapp docker image, `cd` into the root directory of this project (`incubator-sdap-nexus/`) and run:
    
     docker build . -f docker/nexus-webapp/Dockerfile -t nexusjpl/nexus-webapp:distributed.${BUILD_VERSION}

where `${BUILD_VERSION}` is the build version of SDAP.


# Push the image

Push the images to the `nexusjpl` organization on DockerHub

    docker push nexusjpl/nexus-webapp:distributed.${BUILD_VERSION}

# Add a new tag to the Git repo

When you push a new image that is intended for more than just private use, you should also `git tag` the commit from which
the image was built. The tag name should be the docker image tag of the new image you pushed, i.e.:

    git tag distributed.0.3.0
    git push --tags

You can see a list of all SDAP version tags on the [SDAP Git repo tags page](https://github.com/apache/incubator-sdap-nexus/tags).