#!/bin/bash

RELEASE=`grep -1 -A 0 -B 0 '<version>' /vagrant/pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

# In case we are pulling a distro that is built locally
sudo mkdir /usr/local/storm
sudo cp /vagrant/storm-mesos-${RELEASE}.tgz /usr/local/storm

# Start nimbus scheduler
cd /vagrant/_release/storm-mesos-${RELEASE}
bin/storm-mesos nimbus &
