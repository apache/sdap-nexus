scriptdir=`dirname $0`

if [ -z "$1" ]; then
    homedir="/home/vagrant"
else
    homedir=$1
fi

if [ -z "$2" ]; then
    condaenv="nexus-xd-python-modules"
else
    condaenv=$2
fi

pushd $homedir

pushd nexus
git pull https://github.com/dataplumber/nexus.git
popd

source activate $condaenv

# Install spring-xd python module
pushd nexus/nexus-ingest/spring-xd-python
python setup.py install --force
popd

# Install protobuf generated artifacts
pushd nexus/nexus-ingest/nexus-messages
./gradlew clean build writeNewPom

pomfile=`find build/poms/*.xml`
jarfile=`find build/libs/*.jar`
mvn install:install-file -DpomFile=$pomfile -Dfile=$jarfile

pushd build/python/nexusproto
python setup.py install --force
popd
popd

# Install ingestion modules
pushd nexus/nexus-ingest/nexus-xd-python-modules
python setup.py install --force
popd

# Install shared Groovy scripts
pushd nexus/nexus-ingest/groovy-scripts
rm $homedir/xd-nexus-shared/*.groovy
cp *.groovy $homedir/xd-nexus-shared
popd

# Delete all streams in Spring XD so we can update the custom modules
echo stream all destroy --force > /tmp/xdcommand
$homedir/spring-xd-1.3.1.RELEASE/shell/bin/xd-shell --cmdfile /tmp/xdcommand

# Build and upload dataset-tiler
pushd nexus/nexus-ingest/dataset-tiler
./gradlew clean build
jarfile=`find build/libs/*.jar`
touch /tmp/moduleupload
echo module upload --type processor --name dataset-tiler --file $jarfile --force > /tmp/xdcommand
$homedir/spring-xd-1.3.1.RELEASE/shell/bin/xd-shell --cmdfile /tmp/xdcommand
popd

# Build and upload tcp-shell
pushd nexus/nexus-ingest/tcp-shell
./gradlew clean build
jarfile=`find build/libs/*.jar`
touch /tmp/moduleupload
echo module upload --type processor --name tcpshell --file $jarfile --force > /tmp/xdcommand
$homedir/spring-xd-1.3.1.RELEASE/shell/bin/xd-shell --cmdfile /tmp/xdcommand
popd

# Build and upload nexus-sink
pushd nexus/nexus-ingest/nexus-sink
./gradlew clean build
jarfile=`find build/libs/*.jar`
touch /tmp/moduleupload
echo module upload --type sink --name nexus --file $jarfile --force > /tmp/xdcommand
$homedir/spring-xd-1.3.1.RELEASE/shell/bin/xd-shell --cmdfile /tmp/xdcommand
popd

# Create the streams
$homedir/spring-xd-1.3.1.RELEASE/shell/bin/xd-shell --cmdfile $scriptdir/stream-definitions

popd