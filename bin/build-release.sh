#!/bin/bash
set -o errexit -o nounset -o pipefail

function _rm {
  rm -rf "$@" 2>/dev/null || true
}
RELEASE=`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`

MIRROR=${MIRROR:-"http://www.gtlib.gatech.edu/pub"}

function help {
  cat <<USAGE
Usage: bin/build-release.sh <storm.zip>
  clean                   Attempts to clean working files and directories
                            created when building.
  mvnPackage              Runs the maven targets necessary to build the Storm
                            Mesos framework.
  prePackage <storm.zip>  Prepares the working directories to be able to
                            package the Storm Mesos framework.
  package                 Packages the Storm Mesos Framework.
  downloadStormRelease    A utility function to download the Storm release zip
                            for the targeted storm release.
  ENV
    MIRROR        Specify Apache Storm Mirror to download from
                    Default: ${MIRROR}
    RELEASE       The targeted release version of Storm
                    Default: ${RELEASE}
USAGE
}; function --help { help ;}; function -h { help ;}

function downloadStormRelease {
  wget --progress=dot:mega ${MIRROR}/apache/storm/apache-storm-${RELEASE}/apache-storm-${RELEASE}.zip
}

function clean {
  _rm _release
  _rm lib/ classes/
  _rm target
  _rm *mesos*.tgz
}

function mvnPackage {
  mvn package
  mvn dependency:copy-dependencies
}

function prePackage {(
  _rm _release
  mkdir -p _release
  cp $1 _release/storm.zip
  cd _release
  unzip storm.zip
  _rm storm.zip
  mv apache-storm* storm
)}

function package {(
  _rm _release/storm/*.jar

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
  prePackage ${1:-${RELEASE_ZIP_NAME}}
  package
}

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main "$@"
fi
