#!/bin/bash

RELEASE=`grep -1 -A 0 -B 0 '<version>' /vagrant/pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

sudo echo "Starting slave $2" > startslave.log
sudo echo "$2" > /etc/mesos-slave/ip
sudo echo "$2 slave" >> /etc/hosts
sudo echo "$1 master" >> /etc/hosts
sudo echo "zk://$1:2181/mesos" > /etc/mesos/zk

# In case we are pulling a distro that is built locally
sudo mkdir /usr/local/storm
sudo cp /vagrant/storm-mesos-${RELEASE}.tgz /usr/local/storm

sudo start mesos-slave

# a little surprised that the master gets started up
sudo stop mesos-master


