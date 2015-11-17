#!/bin/bash
set -o errexit -o nounset -o pipefail

function _rm {
  rm -rf "$@" 2>/dev/null || true
}

RELEASE=${RELEASE:-`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`}

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
  if [ ! -f apache-storm-${RELEASE}.zip ]; then
      wget --progress=dot:mega ${MIRROR}/apache/storm/apache-storm-${RELEASE}/apache-storm-${RELEASE}.zip
  fi
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
)}

function package {(
  local stormDir=`find _release -maxdepth 1 -type d -name "*storm*"`
  _rm $stormDir/*.jar

  # copies storm-mesos jar over
  cp target/*.jar $stormDir/lib/
  cp bin/storm-mesos $stormDir/bin/
  cp bin/run-with-marathon.sh $stormDir/bin/
  chmod +x $stormDir/bin/*
  mkdir -p $stormDir/native
  cp storm.yaml $stormDir/conf/storm.yaml

  local tarName="storm-mesos-${RELEASE}.tgz"
  cd _release
  # When supervisor starts up it looks for storm-mesos not apache-storm.
  mv apache-storm-${RELEASE} storm-mesos-${RELEASE}
  tar cvzf ${tarName} storm-mesos-${RELEASE}
  echo "Copying ${tarName} to $(cd .. && pwd)/${tarName}"
  cp ${tarName} ../
  cd ..
)}

function dockerImage {(
  rm -rf _docker && \
  mkdir _docker && \
  cp storm-mesos-${RELEASE}.tgz _docker && \
  cd _docker && \
  tar xvf storm-mesos-${RELEASE}.tgz &&
  rm storm-mesos-${RELEASE}.tgz && \
  cd storm-mesos-${RELEASE} && \
  cp ../../Dockerfile . && \
  docker build -t mesos/storm:git-`git rev-parse --short HEAD` . && \
  cd ../../ && \
  rm -rf _docker
)}

function main {
  clean
  downloadStormRelease
  mvnPackage
  prePackage apache-storm-${RELEASE}.zip
  package
}

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main "$@"
fi
