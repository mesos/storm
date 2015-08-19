#!/bin/bash

RELEASE=`grep -1 -A 0 -B 0 '<version>' /vagrant/pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

cd /vagrant/_release/storm-mesos-${RELEASE}
bin/storm jar /vagrant/vagrant/$*
