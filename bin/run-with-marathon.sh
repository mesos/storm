#!/bin/bash
set -o errexit -o nounset -o pipefail

MESOS_MASTER_ZK="${MESOS_MASTER_ZK:-master.mesos}"

echo "Running storm with MESOS_MASTER_ZK=${MESOS_MASTER_ZK}"

function run_ui () {
  while [ true ] ; do
    ./bin/storm ui -c ui.port=$PORT0 -c nimbus.thrift.port=$PORT1 -c storm.log.dir=$MESOS_SANDBOX/logs ${STORM_UI_OPTS:-}
  done
}

function run_nimbus () {
  ./bin/storm-mesos nimbus -c mesos.master.url=zk://${MESOS_MASTER_ZK}:2181/mesos -c storm.zookeeper.servers="[\"${MESOS_MASTER_ZK}\"]" -c nimbus.thrift.port=$PORT1  -c storm.log.dir=$MESOS_SANDBOX/logs  ${STORM_NIMBUS_OPTS:-}
}

function run () {
  run_ui &
  run_nimbus
}

run
