# nexus-messages

This project contains the [protobuf](https://developers.google.com/protocol-buffers/) definition for a NexusTile. By compiling the protobuf specification, both Java and Python objects are generated.

# Developer Installation

1. Run `./gradlew clean build install`

2. cd into `/build/python/nexusproto`

3. Setup a separate conda env or activate an existing one

    ````
    conda create --name nexus-messages python
    source activate nexus-messages
    ````

4. Install Conda dependencies

    ````
    conda install numpy
    ````

5. Run `python setup.py install`
