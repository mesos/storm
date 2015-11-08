#
# Dockerfile for Storm Mesos framework
#
FROM mesosphere/mesos:0.25.0-0.2.70.ubuntu1404
MAINTAINER Timothy Chen <tnachen@gmail.com>

# build packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get install -yq openjdk-7-jdk maven wget

# export environment
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

ADD . /work

WORKDIR /work
RUN ./bin/build-release.sh main
RUN mkdir -p /opt/storm && tar xf storm-mesos-*.tgz -C /opt/storm --strip=1 && rm -rf /work
WORKDIR /opt/storm
