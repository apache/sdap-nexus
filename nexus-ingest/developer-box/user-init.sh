homedir=$1
scriptdir=`dirname $0`

pushd $homedir

mkdir nexus
pushd nexus
git init
popd

mkdir xd-nexus-shared

touch /tmp/xdcommand

popd

$scriptdir/user-update.sh $homedir