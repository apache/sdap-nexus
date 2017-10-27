#!/usr/bin/env bash

scriptdir=`dirname $0`
sharedscripts=/home/vagrant/xd-nexus-shared

rm $sharedscripts/*.groovy
cp $scriptdir/*.groovy $sharedscripts