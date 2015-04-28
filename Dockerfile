#this is oracle java8 atop phusion baseimage
FROM opentable/baseimage-java8:latest




#prep to install libmesos.so
# (this step varies from mesos instructions based on http://unix.stackexchange.com/questions/75892/keyserver-timed-out-when-trying-to-add-a-gpg-public-key)
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv E56151BF

#below echo | tee command is taken from mesos instructions
#DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
#CODENAME=$(lsb_release -cs)
#echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
#  sudo tee /etc/apt/sources.list.d/mesosphere.list
RUN echo "deb http://repos.mesosphere.io/$(lsb_release -is | tr '[:upper:]' '[:lower:]') $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/mesosphere.list
RUN cat /etc/apt/sources.list.d/mesosphere.list

#to make mesos repo available and install mesos:
RUN apt-get update && apt-get install -y \
    mesos \
    python

#install storm
#inspired by https://github.com/wurstmeister/storm-docker/blob/master/storm/Dockerfile
RUN wget -q -O - http://mirrors.sonic.net/apache/storm/apache-storm-0.9.4/apache-storm-0.9.4.tar.gz | tar -xzf - -C /opt

#add our additional jar with mesos nimbus+mesos supervisor
ADD target/mesos-storm-0.9.4.jar /opt/apache-storm-0.9.4/lib/

ENV STORM_HOME /opt/apache-storm-0.9.4
RUN groupadd storm; useradd --gid storm --home-dir /home/storm --create-home --shell /bin/bash storm; chown -R storm:storm $STORM_HOME; mkdir /var/log/storm ; chown -R storm:storm /var/log/storm

RUN ln -s $STORM_HOME/bin/storm /usr/bin/storm

ADD storm.yaml $STORM_HOME/conf/storm.yaml
ADD cluster.xml $STORM_HOME/logback/cluster.xml

#set storm.local.hostname to current container ip
RUN echo "storm.local.hostname: `hostname -i`" >> $STORM_HOME/conf/storm.yaml


#using storm entrypoint
ENTRYPOINT ["storm"]
CMD ["supervisor"]
#start via:
#docker run -d -p 6627:6627 --dns=172.17.42.1 mesos-storm nimbus storm.mesos.MesosNimbus -c mesos.master.url=zk://zk1.service.consul:2181/mesos -c storm.zookeeper.servers=[\"zk1.service.consul\"] -c 'worker.childopts=-Xmx512m -DSTORM_SYSLOG_HOST=172.17.42.1 -DSTORM_SYSLOG_PORT=514' -c some.other.option=some.value

#using default entrypoint:
#CMD storm supervisor
#start nimbus/framework via:
#docker run --rm -it -p 6627:6627 --dns=172.17.42.1 mesos-storm storm nimbus storm.mesos.MesosNimbus -c mesos.master.url=zk://zk1.service.consul:2181/mesos -c storm.zookeeper.servers=[\"zk1.service.consul\"] -c some.other.option=some.value

