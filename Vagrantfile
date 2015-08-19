# -*- mode: ruby -*-
# # vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

$provision_script = <<SCRIPT

PREFIX="PROVISIONER:"

set -e

echo "${PREFIX} Installing pre-reqs..."

# For installing Java 8
add-apt-repository ppa:webupd8team/java

# For Mesos
apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
CODENAME=$(lsb_release -cs)
echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | sudo tee /etc/apt/sources.list.d/mesosphere.list

apt-get -y update
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get -y install oracle-java8-installer
apt-get -y install oracle-java8-set-default
apt-get -y install libcurl3
apt-get -y install zookeeperd
apt-get -y install aria2

echo "${PREFIX}Installing mesos ..."
apt-get -y install mesos
echo "Done"

ln -s /usr/lib/jvm/java-8-oracle/jre/lib/amd64/server/libjvm.so /usr/lib/libjvm.so

echo "${PREFIX}Successfully provisioned machine for storm development"

# Install docker 
apt-get -y install docker.io

SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box_url = "https://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"

  config.vm.provision "shell", inline: $provision_script

  # Configure VM resources
  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
    vb.customize ["modifyvm", :id, "--cpus", "4"]
  end

  config.vm.define "master" do |node|
            node.vm.box = "trusty64-vm-1"
	    node.vm.hostname = "master"
            node.vm.network :private_network, ip: "192.168.50.101"
	    node.vm.network "forwarded_port",  guest: 8080, host: 8080
	    node.vm.network "forwarded_port",  guest: 8000, host: 8000
	    node.vm.network "forwarded_port",  guest: 8081, host: 8081
	    node.vm.network "forwarded_port",  guest: 5050, host: 5050
  	    node.vm.network "forwarded_port",  guest: 5051, host: 5051
            node.vm.provision "shell", path: "vagrant/startmaster.sh", args: ["192.168.50.101","192.168.50.102"]
            node.vm.provision "shell", path: "vagrant/startnimbus.sh", args: "192.168.50.101"
   end

   config.vm.define "slave" do |node|
            node.vm.box = "trusty64-vm-2"
	    node.vm.hostname = "slave"
            node.vm.network :private_network, ip: "192.168.50.102"
	    node.vm.network "forwarded_port",  guest: 8080, host: 80802
	    node.vm.network "forwarded_port",  guest: 8000, host: 80002
	    node.vm.network "forwarded_port",  guest: 8081, host: 80812
	    node.vm.network "forwarded_port",  guest: 5050, host: 50502
  	    node.vm.network "forwarded_port",  guest: 5051, host: 50512
            node.vm.provision "shell", path: "vagrant/startslave.sh", args: ["192.168.50.101","192.168.50.102"]
   end

end
