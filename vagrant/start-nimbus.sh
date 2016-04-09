#!/bin/bash

if [[ $EUID -ne 0 ]]; then
   echo "$0 must be run as root"
   exit 1
fi

RELEASE=`grep -1 -A 0 -B 0 '<version>' /vagrant/pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

echo "`date` - Starting nimbus!!" > start-nimbus.log

cd /vagrant/_release/storm-mesos-${RELEASE}-*

# kill existing MesosNimbus and storm UI processes
kill `ps aux | grep MesosNimbu[s] | awk '{print $2}'` &> /dev/null || /bin/true
# Grr... the below *was* working, but now the jar paths are too long with the long package version name.
kill `ps aux | grep backtype.storm.ui.cor[e] | awk '{print $2}'` &> /dev/null || /bin/true
# So using this more aggressive form now.
kill `ps aux | grep stor[m] | grep -v grep | awk '{print $2}'` &> /dev/null || /bin/true

# Start storm nimbus, which also acts as the mesos scheduler in this case.
# Point the STORM_CONF_DIR to where the repo's storm.yaml lives, so we can modify it
# without having to rebuild the framework tarball and fully reprovision.
MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so STORM_CONF_DIR=/vagrant bin/storm-mesos nimbus &

# Start storm UI
bin/storm ui &
