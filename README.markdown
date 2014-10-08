Storm on Mesos
---------------

[![Build Status](https://travis-ci.org/mesos/storm.svg?branch=master)](https://travis-ci.org/mesos/storm)

# Overview
Storm integration with the Mesos cluster resource manager.

To use a release, you first need to unpack the distribution, fill in configurations listed below into the `conf/storm.yaml` file and start Nimbus using `storm-mesos nimbus`. 

The Mesosphere site has a tutorial which goes into more [details](http://mesosphere.io/learn/run-storm-on-mesos/).

**Note:** It is **not** necessary to repack the distribution - the configuration is automatically pushed out to the slaves from Nimbus.

# Requirements
- Oracle JDK 1.7
OpenJDK 1.7 might work too, but that has not been tested.
The code was compiled with Oracle JDK 1.7 and will not work on Java 1.6.

# Building

Once you have a Storm package, run the build script with the path to the Storm package as an argument:

```shell
bin/build-release.sh storm.zip
```

This will build a Mesos executor package.  You'll need to edit `storm.yaml` and supply the Mesos master configuration as well as the executor package URI (produced by the step above).

## Sub-commands

Sub-commands can be invoked similar to git sub-commands.

For example the following command will download the Storm release zip into the current working directory.
```bash
bin/build-release.sh downloadStormRelease
```

* `clean`

  Attempts to clean working files and directories created when building.

* `mvnPackage`

  Runs the maven targets necessary to build the Storm Mesos framework.

* `prePackage`

  Prepares the working directories to be able to package the Storm Mesos framework.
  * Optional argument specifying the Storm release zip to package against.

* `package`

  Packages the Storm Mesos Framework.

* `downloadStormRelease`

  A utility function to download the Storm release zip for the targeted storm release.

  _Set `MIRROR` environment variable to configure download mirror._

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

There's a minor bug in the UI where it displays the number of slots in the cluster – you don't need to worry about this.

Topologies are submitted to a Storm/Mesos cluster the exact same way they are submitted to a regular Storm cluster.

Storm/Mesos provides resource isolation between topologies. So you don't need to worry about topologies interfering with one another.

## Mandatory configuration

1. `mesos.executor.uri`: Once you fill in the configs and repack the distribution, you need to place the distribution somewhere where Mesos executors can find it. Typically this is on HDFS, and this config is the location of where you put the distibution.

2. `mesos.master.url`: URL for the Mesos master.

3. `storm.zookeeper.servers`: The location of the Zookeeper servers to be used by the Storm master.

4. `nimbus.host`: The hostname of where you run Nimbus.

## Resource configuration

* `topology.mesos.worker.cpu`: CPUs per worker
* `topology.mesos.worker.mem.mb`: Memory (in MiB) per worker
  * `worker.childopts`: Use this for JVM opts.  You should have about 25% memory overhead for each task.  For 
  example, with `-Xmx1000m`, you should set `topology.mesos.worker.mem.mb: 1250`
* `topology.mesos.executor.cpu`: CPUs per executor
* `topology.mesos.executor.mem.mb`: Memory (in MiB) per executor
  * `supervisor.childopts`: Use this for executor (aka supervisor) JVM opts.  You should have about 25% memory 
  overhead for each task.  For example, with `-Xmx500m`, you should set `topology.mesos.executor.mem.mb: 625`
