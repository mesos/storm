# Vagrant setup

You can use following guide to setup a cluster in a virtual machine.

### Prerequisities
* Virtualbox
* Vagrant

### Preparation
* bin/build-release.sh must rename _release/apache-storm-${RELEASE} to _release/storm-mesos-${RELEASE}
* sudo echo "192.168.50.101 master" >> /etc/hosts
* sudo echo "192.168.50.102 slave"  >> /etc/hosts

### Run

```shell
bin/build-release.sh
```

To start the cluster run following:
```shell
vagrant up
```
At this point the VM will have two node mesos cluster running, node 1 (master) will have a mesos master and slave running and node 2 (slave) will have only a mesos slave running. Docker has been installed as well.

To ssh in the cluster run following:
```shell
vagrant ssh master
vagrant ssh slave
``` 
