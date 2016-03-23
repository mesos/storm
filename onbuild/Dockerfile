#
# Dockerfile for Storm Mesos framework
#
FROM mesosphere/mesos:0.27.0-0.2.190.ubuntu1404
MAINTAINER Timothy Chen <tnachen@gmail.com>

ENV DEBIAN_FRONTEND noninteractive

# export environment
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

ONBUILD ARG MESOS_RELEASE=0.27.0
ONBUILD ARG STORM_RELEASE=0.9.6
ONBUILD ARG MIRROR=http://www.gtlib.gatech.edu/pub

ADD . /tmp

RUN apt-get update && \
  apt-get install -y openjdk-7-jdk maven wget
ONBUILD RUN cd /tmp && ./bin/build-release.sh main && \
            mkdir -p /opt/storm && \
            tar xf /tmp/storm-mesos-*.tgz -C /opt/storm --strip=1 && \
            rm -rf /tmp/* ~/.m2 && \
            apt-get -yf autoremove openjdk-7-jdk maven && \
            apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /opt/storm
