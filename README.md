Storm on Mesos
---------------

[![Build Status](https://travis-ci.org/mesos/storm.svg?branch=master)](https://travis-ci.org/mesos/storm)

# Overview
Storm integration with the Mesos cluster resource manager.

To use a release, you first need to unpack the distribution, fill in configurations listed below into the `conf/storm.yaml` file and start Nimbus using `storm-mesos nimbus`.

**Note:** It is **not** necessary to repack the distribution - the configuration is automatically pushed out to the slaves from Nimbus.

# Building

Run build-release.sh to download storm distribution and bundle Storm with this framework into one tar release.

```shell
bin/build-release.sh
```

This will build a Mesos executor package.  You'll need to edit `storm.yaml` and supply the Mesos master configuration as well as the executor package URI (produced by the step above).

## Sub-commands

Sub-commands can be invoked similar to git sub-commands.

For example the following command will download the Storm release tarball into the current working directory.
```bash
bin/build-release.sh downloadStormRelease
```
* `main`

  Build a Storm package with the Mesos scheduler. The output of this command can be used as the package for `mesos.executor.uri`.

* `clean`

  Attempts to clean working files and directories created when building.

* `downloadStormRelease`

  A utility function to download the Storm release tarball for the targeted storm release.

  _Set `MIRROR` environment variable to configure download mirror._

* `mvnPackage`

  Runs the maven targets necessary to build the Storm Mesos framework.

* `prePackage`

  Prepares the working directories to be able to package the Storm Mesos framework.
  * Optional argument specifying the Storm release tarball to package against.

* `package`

  Packages the Storm Mesos Framework.

* `dockerImage`

  Builds a Docker image from the current code.
  * Notably, the mesos/storm repo on Github also has a hook that auomatically builds a [new Docker image on Dockerhub](https://hub.docker.com/r/mesosphere/storm/) upon every commit.

* `help`

  Prints out usage information about the build-release.sh script.

# Running Storm on Mesos
Along with the Mesos master and Mesos cluster, you'll need to run the Storm master as well. Launch Nimbus with this command:

```
bin/storm-mesos nimbus
```

It's recommended that you also run the UI on the same machine as Nimbus via the following command:

```
bin/storm ui
```

There's a minor bug in the UI regarding how it displays the number of slots in the cluster – you don't need to worry about this, it's an artifact of there being no pre-existing slots when Storm runs on Mesos.  Slots are created from available cluster resources when a topology needs its Storm worker processes to be launched.

Topologies are submitted to a Storm/Mesos cluster the exact same way they are submitted to a regular Storm cluster.

Storm/Mesos provides resource isolation between topologies. So you don't need to worry about topologies interfering with one another.

## Vagrant setup

For local development and familiarizing yourself with Storm/Mesos, please see the [Vagrant setup docs](docs/vagrant.md).

## Mandatory configuration

1. One of the following (if both are specified, Docker is preferred):
    * `mesos.executor.uri`: Once you fill in the configs and repack the distribution, you need to place the
      distribution somewhere where Mesos executors can find it. Typically this is on HDFS, and this config is
      the location of where you put the distribution.
    * `mesos.container.docker.image`: You may use a Docker image in place of the executor URI. Take a look at the
      Dockerfile in the top-level of this repository for an example of how to use it.

2. `mesos.master.url`: URL for the Mesos master.

3. `storm.zookeeper.servers`: The location of the ZooKeeper servers to be used by the Storm master.

## Optional configuration

* `mesos.supervisor.suicide.inactive.timeout.secs`: Seconds to wait before supervisor to suicides if
  supervisor has no task to run. Defaults to "120".
* `mesos.master.failover.timeout.secs`: Framework failover timeout in second. Defaults to "24*7*3600".
* `mesos.allowed.hosts`: Allowed hosts to run topology, which takes hostname list as a white list.
* `mesos.disallowed.hosts`: Disallowed hosts to run topology, which takes hostname list as a back list.
* `mesos.framework.role`: Framework role to use. Defaults to "*".
* `mesos.framework.checkpoint`: Enabled framework checkpoint or not. Defaults to false.
* `mesos.offer.lru.cache.size`: LRU cache size. Defaults to "1000".
* `mesos.offer.filter.seconds`: Number of seconds to filter unused Mesos offers. These offers may be revived by
  the framework when needed. Defaults to "120".
* `mesos.offer.expiry.multiplier`: Offer expiry multiplier for `nimbus.monitor.freq.secs`. Defaults to "2.5".
* `mesos.local.file.server.port`: Port for the local file server to bind to. Defaults to a random port.
* `mesos.framework.name`: Framework name. Defaults to "Storm!!!".
* `mesos.framework.principal`: Framework principal to use to register with Mesos
* `mesos.framework.secret.file`:  Location of file that contains the principal's secret. Secret cannot end with a NL.
* `mesos.prefer.reserved.resources`: Prefer reserved resources over unreserved (i.e., `"*"` role). Defaults to "true".
* `supervisor.autostart.logviewer`: Default is true. If you disable the logviewer, you may want to
  subtract 128*1.2 from `topology.mesos.executor.mem.mb` (depending on your settings).

## Resource configuration

* `topology.mesos.worker.cpu`: CPUs per worker. Defaults to "1".
* `topology.mesos.worker.mem.mb`: Memory (in MiB) per worker. Defaults to "1000".
  * `worker.childopts`: Use this for JVM opts.  You should have about 20-25% memory overhead for each task.  For
    example, with `-Xmx1000m`, you should set `topology.mesos.worker.mem.mb: 1200`. By default this is platform
    dependent.
* `topology.mesos.executor.cpu`: CPUs per executor. Defaults to "0.1".
* `topology.mesos.executor.mem.mb`: Memory (in MiB) per executor. Defaults to "500".
  * `supervisor.childopts`: Use this for executor (aka supervisor) JVM opts.  You should have about 20-25% memory
    overhead for each task.  For example, with `-Xmx500m`, you should set `topology.mesos.executor.mem.mb: 620`. By
    default this is platform dependent.

## Running Storm on Marathon

To get started quickly, you can run Storm on Mesos with Marathon and Docker, provided you have Mesos-DNS configured
in your cluster. If you're not using Mesos-DNS, set the MESOS_MASTER_ZK environment variable to point to your
ZooKeeper cluster. Included is a script (`bin/run-with-marathon.sh`) which sets the necessary config parameters,
and starts the UI and Nimbus. Since Storm writes stateful data to disk, you may want to consider mounting an
external volume for the `storm.local.dir` config param, and pinning Nimbus to a particular host. You can run this
from Marathon, using the example app JSON below:

```json
{
  "id": "storm-nimbus",
  "cmd": "./bin/run-with-marathon.sh",
  "cpus": 1.0,
  "mem": 1024,
  "ports": [0, 1],
  "instances": 1,
  "container": {
    "type": "DOCKER",
    "docker": {
      "image": "mesosphere/storm",
      "network": "HOST",
      "forcePullImage":true
    }
  },
  "healthChecks": [
    {
      "protocol": "HTTP",
      "portIndex": 0,
      "path": "/",
      "gracePeriodSeconds": 120,
      "intervalSeconds": 20,
      "maxConsecutiveFailures": 3
    }
  ]
}
```
## Running an example topology

Once Nimbus is running, you can launch one of the `storm-starter` topologies. In order to submit the topology
to Nimbus, you'll need to know the Thrift host and API port. In the Marathon example above, the port will be the second
one assigned by Marathon. For example, if the host is `10.0.0.1` and second port is `32001`, run:

```
$ ./bin/storm jar -c nimbus.host=10.0.0.1 -c nimbus.thrift.port=32001 examples/storm-starter/storm-starter-topologies-0.9.6.jar storm.starter.WordCountTopology word-count
```

## Running without Marathon

If you'd like to run the example above _without_ Marathon, you can do so by specifying 2 required ports, the
MESOS_SANDBOX path, and running the container. For example:

```
$ docker run -i --net=host -e PORT0=10000 -e PORT1=10001 -e MESOS_SANDBOX=/var/log/storm -t mesosphere/storm ./bin/run-with-marathon.sh
```
