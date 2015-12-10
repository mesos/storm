# Vagrant setup

You can use following guide to setup a cluster in a virtual machine.

### Prerequisities
* Virtualbox
* Vagrant

### Preparation
1. Add name lookup entries for the 2 VMs:

  ```shell
  sudo sh -c 'echo "192.168.50.101 master" >> /etc/hosts'
  sudo sh -c 'echo "192.168.50.102 slave"  >> /etc/hosts'
  ```

2. Update `storm.yaml`, specifically ensuring you modify these fields:

  ```shell
  mesos.master.url: "zk://master:2181/mesos"
  storm.zookeeper.servers:
  - "master"
  # Modify package version as appropriate
  mesos.executor.uri: "file:///usr/local/storm/storm-mesos-0.9.6.tgz"
  # Disable docker container setting, since we're launching the executor directly:
  #mesos.container.docker.image: mesosphere/storm
  ```

### Run

```shell
bin/build-release.sh
```

To start the cluster run following:
```shell
vagrant up
```
This will bring up a 1-node mesos cluster:
* 'master' : will have a mesos-master and mesos-slave running.

Notably, you can bring up a 2nd node in the cluster to act as another worker host that runs mesos-slave.  To do this you must set `enable_second_slave` to `true` in the `Vagrantfile` and then:
```shell
vagrant up slave
```

To ssh into the cluster hosts, run the following commands:
```shell
vagrant ssh master
vagrant ssh slave
```

Then you can submit a storm topology to the nimbus from outside the guest VMs like so:
```shell
cp my-topology.jar vagrant/.
vagrant ssh master -c "/vagrant/vagrant/submit-topology.sh my-topology.jar foo.bar.TopologyMain optionalArgument1 optionalArgument2 ..."
```

### VM Setup Notes

Docker has been installed.

##### Shared Directory
Notably, `/vagrant/` in the guest VMs is a mount of the repo's basedir on the outer host. i.e.,
```shell
# on host, assuming the repo's basedir is called "mesos-storm" and it's in the homedir of user 'foobar':
/Users/foobar/mesos-storm/

# maps to the following path in the VMs:
/vagrant/
```

This shared directory is used by the vagrant provisioning scripts to deploy the framework tarball into the guest VMs.

### Cluster Web Interfaces

* [Storm Nimbus UI](http://master:8080)
* [Mesos UI](http://master:5050)

### Iterating

As you make changes to the framework code and want to test them, you can follow these steps to iterate:

```shell
# 1. build the framework tarball, which also resets the storm nimbus state for the master VM
#    (since it is stored in `_release/storm-mesos-*/storm-local`, which is removed by the `clean` build function)
bin/build-release.sh

# 2. reprovision the VMs to reset mesos-slave state and start up newly built storm-mesos framework
vagrant provision

# 3. submit a topology
vagrant ssh master -c "/vagrant/vagrant/submit-topology.sh my-topology.jar foo.bar.TopologyMain optionalArgument1 optionalArgument2 ..."
```

Alternatively, you might want to quickly mess with the `storm.yaml` config values that affect the MesosNimbus, MesosSupervisor and storm workers.  This can be done with the process above, but it is a bit slow and wipes out the existing state of the cluster.  Instead you might consider the following process for relaunching the MesosNimbus with a modified `storm.yaml` that will be propagated to new MesosSupervisor and storm worker processes:

```shell
# 1. Modify storm.yaml in repo's base dir
vim storm.yaml

# 2. Relaunch MesosNimbus with new storm.yaml being read in:
vagrant ssh master -c "sudo nohup /vagrant/vagrant/start-nimbus.sh"

# 3. (optional) Existing MesosSupervisor and storm worker processes will not respect the
#     new storm.yaml, so you may want to kill those existing processes manually. You
#     need only kill the MesosSupervisor process(es) and the storm workers will die as
#     well.  The nimbus will then relaunch the processes automatically.
```

### Debugging:
WARNING: If you do not correctly configure `storm.yaml` by setting all of the mandatory options specified in this project's README.md, then various things will break, and it may be hard to figure out exactly what is at fault.

Various logs and files can be accessed from within the VM:
```shell
# Storm Nimbus:
/vagrant/_release/storm-mesos-*/logs/nimbus.log

# Storm UI:
/vagrant/_release/storm-mesos-*/logs/ui.log

# mesos-master
/var/log/mesos/mesos-master.INFO

# mesos-slave
/var/log/mesos/mesos-slave.INFO

# For below mesos-executor related logs, replace the following variables:
#  SLAVE_ID - the ID of the mesos-slave instance
#  FRAMEWORK_ID - the ID of the framework you are running.  Just use the newest dir in that location.
#  TOPOLOGY_ID - the ID of the storm topology you are running. Note that this is *not* simply the topology *name*, but the *ID*.
# Notably, we attempt to wipe out the mesos-slave work_dir on every vagrant provision, thus allowing
# use of wildcards instead of requiring finding latest ID values.

# Mesos Executor:
/tmp/mesos/slaves/SLAVE_ID/frameworks/FRAMEWORK_ID/executors/TOPOLOGY_ID/runs/latest/{stderr,stdout}
/tmp/mesos/slaves/*/frameworks/*/executors/*/runs/latest/{stderr,stdout}
# NOTE: if you see the following line in stderr, then you've probably incorrectly configured storm.yaml:
#   Failed to load native Mesos library from null
#  e.g., leaving mesos.container.docker.image set when not using docker, or not setting mesos.executor.uri

# Storm Supervisor:
/tmp/mesos/slaves/SLAVE_ID/frameworks/FRAMEWORK_ID/executors/TOPOLOGY_ID/runs/latest/storm-mesos-*/logs/*supervisor.log
/tmp/mesos/slaves/*/frameworks/*/executors/*/runs/latest/storm-mesos-*/logs/*supervisor.log

# Storm Worker:
/tmp/mesos/slaves/SLAVE_ID/frameworks/FRAMEWORK_ID/executors/TOPOLOGY_ID/runs/latest/storm-mesos-*/logs/*worker-*.log
/tmp/mesos/slaves/*/frameworks/*/executors/*/runs/latest/storm-mesos-*/logs/*worker-*.log

# Storm Logviewer:
/tmp/mesos/slaves/SLAVE_ID/frameworks/FRAMEWORK_ID/executors/TOPOLOGY_ID/runs/latest/storm-mesos-*/logs/logviewer.log
/tmp/mesos/slaves/*/frameworks/*/executors/*/runs/latest/storm-mesos-*/logs/logviewer.log

# storm.yaml downloaded for the MesosSupervisor and worker:
/tmp/mesos/slaves/SLAVE_ID/frameworks/FRAMEWORK_ID/executors/TOPOLOGY_ID/runs/latest/storm.yaml
/tmp/mesos/slaves/*/frameworks/*/executors/*/runs/latest/storm.yaml
```

### Storm Nimbus working dir

Where the working dir is when viewed from within the master VM:

```shell
# The cwd / working-directory of the MesosNimbus process, where its logs and state are stored:
/vagrant/_release/storm-mesos-*/

# e.g., the state:
/vagrant/_release/storm-mesos-*/storm-local/
```

### Mesos configs:
This setup runs on ubuntu with upstart.  For information about where the configs are located and where you can add them, please see the comment block at the top of the `mesos-init-wrapper` script. e.g.,
```shell
vagrant@master:~$ sudo /usr/bin/mesos-init-wrapper -h
 USAGE: mesos-init-wrapper (master|slave)

  Run Mesos in master or slave mode, loading environment files, setting up
  logging and loading config parameters as appropriate.

  To configure Mesos, you have many options:

 *  Set a Zookeeper URL in

      /etc/mesos/zk

    and it will be picked up by the slave and master.

 *  You can set environment variables (including the MESOS_* variables) in
    files under /etc/default/:

      /etc/default/mesos          # For both slave and master.
      /etc/default/mesos-master   # For the master only.
      /etc/default/mesos-slave    # For the slave only.

 *  To set command line options for the slave or master, you can create files
    under certain directories:

      /etc/mesos-slave            # For the slave only.
      /etc/mesos-master           # For the master only.

    For example, to set the port for the slave:

      echo 5050 > /etc/mesos-slave/port

    To set the switch user flag:

      touch /etc/mesos-slave/?switch_user

    To explicitly disable it:

      touch /etc/mesos-slave/?no-switch_user

    Adding attributes and resources to the slaves is slightly more granular.
    Although you can pass them all at once with files called 'attributes' and
    'resources', you can also set them by creating files under directories
    labeled 'attributes' or 'resources':

      echo north-west > /etc/mesos-slave/attributes/rack

    This is intended to allow easy addition and removal of attributes and
    resources from the slave configuration.
```

### ZooKeeper configs & data:
```shell
# config dir
/etc/zookeeper/conf/

# data dir
/var/lib/zookeeper/
```
