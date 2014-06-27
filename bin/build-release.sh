#!/bin/bash

set -o errexit -o nounset -o pipefail
set -x

RELEASE=`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

echo `rm -rf _release`
echo `rm -rf lib/ classes/`
echo `rm -rf target`
echo `rm -f *mesos*.tgz`

echo `mvn package`
echo `mvn dependency:copy-dependencies`

echo `rm -rf _release`
echo `mkdir -p _release`
echo `cp $1 _release/storm.zip`
cd _release
echo `unzip storm.zip`
echo `rm storm.zip`
echo `mv storm* storm`
cd ..
echo `rm _release/storm/*.jar`

# copies storm-mesos jar over
echo `cp target/*.jar _release/storm/lib/`

echo `cp bin/storm-mesos _release/storm/bin/`

echo `mkdir -p _release/storm/native`

echo `cp storm.yaml _release/storm/conf/storm.yaml`

cd _release
echo `mv storm storm-mesos-$RELEASE`
echo `tar czf storm-mesos-$RELEASE.tgz storm-mesos-$RELEASE`
echo `cp storm-mesos-$RELEASE.tgz ../`
cd ..
#echo `rm -rf _release`
