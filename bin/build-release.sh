#!/bin/bash

# Uncomment to debug this script
#set -x

set -o errexit -o nounset -o pipefail

function _rm {
  rm -rf "$@" 2>/dev/null || true
}

RELEASE=${RELEASE:-`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`}

STORM_RELEASE=${STORM_RELEASE:-`grep -1 -A 0 -B 0 '<storm.default.version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<storm.default.version>//' | sed -e 's/<\/storm.default.version>.*//'`}

if [[ $STORM_RELEASE == "0.10"* ]]; then
  PROFILE=storm10
else
  PROFILE=storm9
fi

MESOS_RELEASE=${MESOS_RELEASE:-`grep -1 -A 0 -B 0 '<mesos.default.version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<mesos.default.version>//' | sed -e 's/<\/mesos.default.version>.*//'`}

STORM_URL=${STORM_URL:-''}

MIRROR=${MIRROR:-"apache.cs.utah.edu"}

DOCKER_REPO=${DOCKER_REPO:-"mesos/storm"}

JAVA_PRODUCT_VERSION=${JAVA_PRODUCT_VERSION:-`java -version 2>&1 | awk '/version/{print $NF}' | sed -E 's|[0-9].([0-9]).[0-9]_[0-9]+|\1|' | sed -e 's/^"//'  -e 's/"$//'`}

function help {
  cat <<USAGE
Usage: bin/build-release.sh [<storm.tar.gz>]
  clean                     Cleans working files and directories created when
                              building.
  mvnPackage                Runs the maven targets necessary to build the Storm
                              Mesos framework.
  prePackage <storm.tar.gz> Prepares the working directories to be able to
                              package the Storm Mesos framework.
  package                   Packages the Storm Mesos Framework.
  downloadStormRelease      A utility function to download the Storm release zip
                              for the targeted storm release.
  dockerImage               Build a dockerImage from the current code. Not
                              part of the standard steps from a raw invocation of
                              bin/build-release.sh.

  ENV
    MIRROR          Specify Apache Storm Mirror to download from
                      Default: ${MIRROR}
    STORM_RELEASE   The targeted release version of Storm
                      Default: ${STORM_RELEASE}
    MESOS_RELEASE   The targeted release version of Mesos
                      Default: ${MESOS_RELEASE}
    STORM_URL       Override the URL for downloading the storm binary
                      NOTE: ensure version matches with STORM_RELEASE

USAGE
}; function --help { help ;}; function -h { help ;}

function downloadStormRelease {
  if [ ! -f apache-storm-${STORM_RELEASE}*.tar.gz ]; then
    if [ -z ${STORM_URL} ]; then
      curl -L -O ${MIRROR}/storm/apache-storm-${STORM_RELEASE}/apache-storm-${STORM_RELEASE}.tar.gz
    else
      curl -L -O ${STORM_URL}
    fi
  fi
}

function clean {
  _rm _release
  _rm lib/ classes/
  _rm target
  _rm *mesos*.tgz
  _rm *storm*tar.gz
}

function mvnPackage {
  mvn clean package -P$PROFILE -Dstorm.version=$STORM_RELEASE -Dmesos.version=$MESOS_RELEASE
}

function prePackage {(
  _rm _release
  mkdir -p _release
  cp $1 _release/.
  cd _release
  tar xvf $1
  _rm $1
)}

function package {(
  local stormDir=`find _release -maxdepth 1 -type d -name "*storm*"`
  _rm $stormDir/*.jar

  # for non-docker development builds, create a tarball of the source and copy into the package
  if [[ "${RELEASE}" == *"SNAPSHOT"* && ! -f /.dockerenv ]]; then
    echo "bundling source in!!"
    local stashRef=$(git stash create)
    local archiveRef=${stashRef}
    if [ -z ${stashRef} ]; then
      archiveRef=HEAD
    fi
    local srcTarballDir=storm-mesos-src-${archiveRef}
    local srcTarball=${srcTarballDir}.tgz
    git archive --format=tar.gz -o ${srcTarball} --prefix ${srcTarballDir}/ ${archiveRef}
    echo "Copying ${srcTarball} to $stormDir/."
    cp ${srcTarball} $stormDir/.
  fi

  # copies storm-mesos jar over
  # We only want the shaded jar. Its important to remove the original
  # jar so we dont have both shaded as well as original jar in the classpath
  # for mesos nimbus
  cp storm/target/storm-mesos-${RELEASE}-storm${STORM_RELEASE}-mesos${MESOS_RELEASE}.jar $stormDir/lib
  cp bin/storm-mesos $stormDir/bin/
  cp bin/run-with-marathon.sh $stormDir/bin/
  chmod +x $stormDir/bin/*
  mkdir -p $stormDir/native
  cp storm.yaml $stormDir/conf/storm.yaml

  local dirName="storm-mesos-${RELEASE}-storm${STORM_RELEASE}-mesos${MESOS_RELEASE}"
  local tarName="${dirName}.tgz"
  cd _release
  # When supervisor starts up it looks for storm-mesos not apache-storm.
  mv apache-storm-${STORM_RELEASE}* ${dirName}
  tar cvzf ${tarName} --numeric-owner --owner 0 --group 0 ${dirName}
  echo "Copying ${tarName} to $(cd .. && pwd)/${tarName}"
  cp ${tarName} ../

  # create logs dir for MesosNimbus to use -- when you rebuild this package and are using the
  # vagrant setup in this repo, the virtualbox shared-file driver gets confused about whether
  # the logs dir is really there or not, since it is being deleted when we rebuild.
  mkdir ${dirName}/logs

  cd ..
)}

function dockerImage {(
  cmd="make images \
      STORM_RELEASE=$STORM_RELEASE \
      MESOS_RELEASE=$MESOS_RELEASE \
      RELEASE=$RELEASE \
      MIRROR=$MIRROR \
      STORM_URL=$STORM_URL \
      JAVA_PRODUCT_VERSION=$JAVA_PRODUCT_VERSION \
      DOCKER_REPO=$DOCKER_REPO"
  echo $cmd
  $cmd
)}

# Ensure we have GNU tar so that we can use options such as --owner, etc.
function ensureCorrectTar {(
  if ! tar --version | grep -q 'GNU tar'; then
    echo "ERROR: 'tar' command is not from GNU.
         Assuming you are on Mac OS X please use homebrew to install gnu-tar and ensure that 'tar' resolves to the
         tar binary from that package.  Please see this page for more info:
           https://apple.stackexchange.com/questions/69223/how-to-replace-mac-os-x-utilities-with-gnu-core-utilities
         The following steps *may* work for you, assuming you have homebrew:
           brew uninstall gnu-tar   # in case you've previously installed gnu-tar but the binary is named 'gtar' instead of 'tar'
           brew install --with-default-names gnu-tar  # install gnu-tar as 'tar'
           hash -r  # resets shell's recorded command paths"
    exit 1
  fi
)}

function main {
  clean
  downloadStormRelease
  mvnPackage
  prePackage apache-storm-${STORM_RELEASE}*.tar.gz
  package
}

ensureCorrectTar

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main "$@"
fi
