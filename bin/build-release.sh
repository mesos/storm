#!/bin/bash
set -o errexit -o nounset -o pipefail

RELEASE=`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

RELEASE_ZIP_NAME=apache-storm-${RELEASE}.zip

MIRROR=http://www.gtlib.gatech.edu/pub
function downloadStormRelease {
  wget --progress=dot:mega ${MIRROR}/apache/incubator/storm/apache-storm-${RELEASE}/apache-storm-${RELEASE}.zip
}

function clean {
  rm -rf _release || true
  rm -rf lib/ classes/ || true
  rm -rf target || true
  rm -f *mesos*.tgz || true
}

function mvnPackage {
  mvn package
  mvn dependency:copy-dependencies
}

function release {(
  rm -rf _release
  mkdir -p _release
  cp $1 _release/storm.zip
  cd _release
  unzip storm.zip
  rm storm.zip
  mv apache-storm* storm
)}

function package {(
  rm _release/storm/*.jar 2> /dev/null || true

  # copies storm-mesos jar over
  cp target/*.jar _release/storm/lib/
  cp bin/storm-mesos _release/storm/bin/
  mkdir -p _release/storm/native
  cp storm.yaml _release/storm/conf/storm.yaml

  local tarName="storm-mesos-${RELEASE}.tgz"
  cd _release
  mv storm storm-mesos-${RELEASE}
  tar czf ${tarName} storm-mesos-${RELEASE}
  echo "Copying ${tarName} to $(cd .. && pwd)/${tarName}"
  cp ${tarName} ../
)}

function main {
  clean
  mvnPackage
  release ${1:-${RELEASE_ZIP_NAME}}
  package
}

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main "$@"
fi
