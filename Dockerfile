#
# Dockerfile for Storm Mesos framework
#
FROM mesosphere/mesos:0.22.0-1.0.ubuntu1404
MAINTAINER Timothy Chen <tnachen@gmail.com>

# build packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update
RUN apt-get install -yq openjdk-7-jdk

# export environment
ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/lib/libmesos.so

# copy local checkout into /opt/storm
ADD . /opt/storm

WORKDIR /opt/storm

ENTRYPOINT ["/opt/storm/bin/storm-mesos"]
