# Install JDK
sudo yum -y install java-1.8.0-openjdk*

# Install maven
echo "Downloading Maven"
curl "http://mirror.stjschools.org/public/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz" -s -O
tar xvf apache-maven-3.3.9-bin.tar.gz
sudo mv apache-maven-3.3.9  /usr/local/apache-maven
cat >> /home/vagrant/.bashrc << END
# add for maven install
M2_HOME=/usr/local/apache-maven
M2=\$M2_HOME/bin
PATH=\$M2:\$PATH
END

# Install nco tools
sudo yum -y install epel-release
sudo yum -y install nco-devel

# Install git
sudo yum -y install git

# Install and configure Anaconda
echo "Downloading Anaconda"
curl "https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh" -s -O

chmod +x Anaconda2-4.3.0-Linux-x86_64.sh

./Anaconda2-4.3.0-Linux-x86_64.sh -b -p /home/vagrant/anaconda2

cat >> /home/vagrant/.bashrc << END
# add for anaconda install
PATH=/home/vagrant/anaconda2/bin:\$PATH
END

source /home/vagrant/.bashrc

conda create -y --name nexus-xd-python-modules python
source activate nexus-xd-python-modules
conda install -y scipy=0.18.1 netcdf4


# Install Spring XD and launch it

sudo yum -y install unzip

echo "Downloading Spring XD"
curl "http://repo.spring.io/libs-release/org/springframework/xd/spring-xd/1.3.1.RELEASE/spring-xd-1.3.1.RELEASE-dist.zip" -s -O

unzip spring-xd-1.3.1.RELEASE-dist.zip
mkdir spring-xd-1.3.1.RELEASE/xd/lib/none

sudo ln -s /home/vagrant/sync /vagrant
cp /vagrant/xd-singlenode-logback.groovy spring-xd-1.3.1.RELEASE/xd/config/xd-singlenode-logback.groovy

(crontab -l 2>/dev/null; echo "@reboot /vagrant/start-singlenode-at-boot.sh") | crontab -

mkdir /vagrant/logs
pushd /home/vagrant
nohup ./spring-xd-1.3.1.RELEASE/xd/bin/xd-singlenode --hadoopDistro none > /vagrant/logs/xd-singlenode.log 2>&1 &
popd