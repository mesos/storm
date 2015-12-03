#
# Dockerfile for Storm Mesos framework
#
FROM mesosphere/mesos:0.25.0-0.2.70.ubuntu1404
MAINTAINER Timothy Chen <tnachen@gmail.com>

ENV DEBIAN_FRONTEND noninteractive

# export environment
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

ADD . /work

WORKDIR /work

RUN apt-get update && \
  apt-get install -yq openjdk-7-jdk maven wget && \
  ./bin/build-release.sh main && \
  mkdir -p /opt/storm && \
  tar xf storm-mesos-*.tgz -C /opt/storm --strip=1 && \
  rm -rf /work ~/.m2 && \
  apt-get -yf autoremove openjdk-7-jdk maven && \
  apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/storm
