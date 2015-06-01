#!/bin/bash

sudo echo "Starting master!! $@" > startmaster.log

sudo echo "zk://$1:2181/mesos" > /etc/mesos/zk
sudo echo "$1 master" >> /etc/hosts
sudo echo "$2 slave" >> /etc/hosts
sudo echo $1 > /etc/mesos-master/ip
sudo echo $1 > /etc/mesos-slave/ip

sudo start mesos-master 
sudo start mesos-slave
