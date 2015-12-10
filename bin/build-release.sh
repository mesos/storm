#!/bin/bash

# Uncomment to debug this script
#set -x

set -o errexit -o nounset -o pipefail

function _rm {
  rm -rf "$@" 2>/dev/null || true
}

RELEASE=${RELEASE:-`grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`}

MIRROR=${MIRROR:-"http://www.gtlib.gatech.edu/pub"}

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
    MIRROR        Specify Apache Storm Mirror to download from
                    Default: ${MIRROR}
    RELEASE       The targeted release version of Storm
                    Default: ${RELEASE}
USAGE
}; function --help { help ;}; function -h { help ;}

function downloadStormRelease {
  if [ ! -f apache-storm-${RELEASE}.tar.gz ]; then
      wget --progress=dot:mega ${MIRROR}/apache/storm/apache-storm-${RELEASE}/apache-storm-${RELEASE}.tar.gz
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
  cp $1 _release/.
  cd _release
  tar xvf $1
  _rm $1
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

  tar cvzf ${tarName} --numeric-owner --owner 0 --group 0 storm-mesos-${RELEASE}
  echo "Copying ${tarName} to $(cd .. && pwd)/${tarName}"
  cp ${tarName} ../

  # create logs dir for MesosNimbus to use -- when you rebuild this package and are using the
  # vagrant setup in this repo, the virtualbox shared-file driver gets confused about whether
  # the logs dir is really there or not, since it is being deleted when we rebuild.
  mkdir storm-mesos-${RELEASE}/logs

  cd ..
)}

function dockerImage {(
  docker build -t mesos/storm:git-`git rev-parse --short HEAD`
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
  prePackage apache-storm-${RELEASE}.tar.gz
  package
}

ensureCorrectTar

######################### Delegates to subcommands or runs main, as appropriate
if [[ ${1:-} ]] && declare -F | cut -d' ' -f3 | fgrep -qx -- "${1:-}"
then "$@"
else main "$@"
fi
