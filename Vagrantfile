# -*- mode: ruby -*-
# # vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

$provision_script = <<SCRIPT

function install_package {
  name_of_package=$1
  echo "Installing ${name_of_package}"
  apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install ${name_of_package}
}

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

apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y update
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections

echo "Installing oracle-java8-installer..."
install_package "oracle-java8-installer"
echo "Installing oracle-java8-set-default..."
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install oracle-java8-set-default
echo "Installing libcurl3..."
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install libcurl3
echo "Installing zookeeperd..."
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install zookeeperd
echo "Installing aria2..."
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install aria2

echo "${PREFIX}Installing mesos ..."
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install mesos=0.25.0-0.2.70.ubuntu1404
echo "Done"

ln -sf /usr/lib/jvm/java-8-oracle/jre/lib/amd64/server/libjvm.so /usr/lib/libjvm.so

echo "${PREFIX}Successfully provisioned machine for storm development"

# Install docker
apt-get -o Acquire::http::Timeout=1 -o Acquire::ftp::Timeout=1 -y install docker.io

SCRIPT

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  master_ip = "192.168.50.101"
  slave_ip  = "192.168.50.102"
  # By default we only launch the 1st VM ("master") which has all the needed components:
  #  zookeeper, mesos-master, mesos-slave, storm UI, mesos-nimbus
  # However, you may want to enable the 2nd VM to better emulate a real cluster.
  enable_second_slave = false

  config.vm.box_url = "https://cloud-images.ubuntu.com/vagrant/trusty/current/trusty-server-cloudimg-amd64-vagrant-disk1.box"

  # Configure VM resources
  config.vm.provider :virtualbox do |vb|
    vb.customize ["modifyvm", :id, "--memory", "4096"]
    vb.customize ["modifyvm", :id, "--cpus", "4"]
  end

  # Prevent "default: stdin: is not a tty" error
  config.vm.provision "fix-no-tty", type: "shell" do |s|
    s.privileged = false
    s.inline = "sudo sed -i '/tty/!s/mesg n/tty -s \\&\\& mesg n/' /root/.profile"
  end

  config.vm.provision "shell", inline: $provision_script

  config.vm.define "master" do |node|
    node.vm.box = "trusty64-vm-1"
    node.vm.hostname = "master"
    node.vm.network :private_network, ip: master_ip
    # storm UI port
    node.vm.network "forwarded_port",  guest: 8080, host: 8080
    # nimbus thrift port
    node.vm.network "forwarded_port",  guest: 6627, host: 6627
    node.vm.provision "shell", path: "vagrant/start-mesos-master-and-slave.sh", args: [master_ip, slave_ip]
    node.vm.provision "shell", path: "vagrant/start-nimbus.sh"
    node.vm.provision "shell", inline: "echo 'Sleeping for 30 seconds, it can take some time for " +
                                       "mesos and storm to fully come up, though 30 seconds might not " +
                                       "be enough. Patience is a virtue, but if it is longer than 2 " +
                                       "minutes and the UI links are not working, then you should " +
                                       "proceed to debugging.'" +
                                       "; sleep 30"
  end

  if enable_second_slave
    config.vm.define "slave" do |node|
      node.vm.box = "trusty64-vm-2"
      node.vm.hostname = "slave"
      node.vm.network :private_network, ip: slave_ip
      node.vm.provision "shell", path: "vagrant/start-mesos-slave.sh", args: [master_ip, slave_ip]
    end
  end

end
