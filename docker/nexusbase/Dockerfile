FROM centos:7

WORKDIR /tmp

RUN yum -y update && \
    yum -y install wget \
    git \
    which \
    bzip2

# Install Oracle JDK 1.8u121-b13
RUN wget -q --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.rpm" && \
    yum -y install jdk-8u121-linux-x64.rpm && \
    rm jdk-8u121-linux-x64.rpm
ENV JAVA_HOME /usr/java/default

# ########################
# # Apache Maven   #
# ########################
ENV M2_HOME /usr/local/apache-maven
ENV M2 $M2_HOME/bin 
ENV PATH $PATH:$M2

RUN mkdir $M2_HOME && \
    wget -q http://mirror.stjschools.org/public/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz && \
    tar -xvzf apache-maven-3.3.9-bin.tar.gz -C $M2_HOME --strip-components=1 && \
    rm -f apache-maven-3.3.9-bin.tar.gz

# ########################
# # Anaconda   #
# ########################
RUN wget -q https://repo.continuum.io/archive/Anaconda2-4.3.0-Linux-x86_64.sh -O install_anaconda.sh && \
    /bin/bash install_anaconda.sh -b -p /usr/local/anaconda2 && \
    rm install_anaconda.sh
ENV PATH $PATH:/usr/local/anaconda2/bin

