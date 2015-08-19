#!/bin/bash

if [[ $EUID -ne 0 ]]; then
   echo "$0 must be run as root"
   exit 1
fi

RELEASE=`grep -1 -A 0 -B 0 '<version>' /vagrant/pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

echo "`date` - Starting mesos-master & mesos-slave!! $@" > start-mesos-master-and-slave.log

# enable running of docker images
echo 'docker,mesos' > /etc/mesos-slave/containerizers
echo "zk://$1:2181/mesos" > /etc/mesos/zk
grep -q "$1 master" /etc/hosts || echo "$1 master" >> /etc/hosts
grep -q "$2 slave" /etc/hosts || echo "$2 slave" >> /etc/hosts
echo $1 > /etc/mesos-master/ip
echo $1 > /etc/mesos-slave/ip

# Allow executor to be loaded from the tarball built by build-release.sh
mkdir -p /usr/local/storm
rm -rf /usr/local/storm/* || /bin/true
cp /vagrant/storm-mesos-${RELEASE}.tgz /usr/local/storm

# clean up previous runs' mesos-slave work_dir
rm -rf /tmp/mesos/*

# Restart (or start) mesos-master & mesos-slave after the configurations above
status mesos-master | grep -q running && restart mesos-master || start mesos-master
status mesos-slave | grep -q running && restart mesos-slave || start mesos-slave
