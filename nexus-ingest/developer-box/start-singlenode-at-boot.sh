source activate nexus-xd-python-modules
pushd /home/vagrant
nohup ./spring-xd-1.3.1.RELEASE/xd/bin/xd-singlenode --hadoopDistro none > /vagrant/logs/xd-singlenode.log 2>&1 &
popd
