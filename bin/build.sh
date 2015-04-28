#!/bin/bash

#build jar
mvn clean package

#build docker images
docker build -t mesos-storm .