FROM mesosphere/mesos-slave:0.22.0-1.0.ubuntu1404

RUN apt-get update && apt-get install -y apt-transport-https

RUN echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
RUN apt-get update && apt-get install -y --force-yes lxc-docker