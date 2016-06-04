#
# Makefile for building storm docker images
#
# make help
#
#
RELEASE ?= `grep -1 -A 0 -B 0 '<version>' pom.xml | head -n 1 | awk '{print $1}' | sed -e 's/.*<version>//' | sed -e 's/<\/version>.*//'`
DOCKER_REPO ?= mesos
MIRROR ?=
STORM_URL ?=
JAVA_PRODUCT_VERSION ?= 7

all: help

help:
	@echo 'Options available:'
	@echo '  make images STORM_RELEASE=0.10.1 MESOS_RELEASE=0.28.2 DOCKER_REPO=mesos'
	@echo '  make push   STORM_RELEASE=0.10.1 MESOS_RELEASE=0.28.2 DOCKER_REPO=mesos'
	@echo ''
	@echo 'ENV'
	@echo '  STORM_RELEASE          The targeted release version of Storm'
	@echo '                             Default: 0.10.1'
	@echo '  MESOS_RELEASE          The targeted release version of MESOS'
	@echo '                             Default: 0.28.2'
	@echo '  DOCKER_REPO            The docker repo for which to build the docker image'
	@echo '                             Default: mesos'
	@echo '  RELEASE                The targeted release version of Storm'
	@echo '                             Default: current version in pom.xml'
	@echo '  JAVA_PRODUCT_VERSION   The java product version to use in the docker image'
	@echo '                             Default: 7'
	@echo '  MIRROR                 Where to download the apache storm packages'
	@echo '                             Default: http://www.gtlib.gatech.edu/pub'
	@echo '  STORM_URL              The url to use to download the apache storm packages'
	@echo '                             Default: $$MIRROR/apache/storm/apache-storm-$$STORM_RELEASE/apache-storm-$$STORM_RELEASE.tar.gz'

check-version:
ifndef STORM_RELEASE
	@echo "Error: STORM_RELEASE is undefined."
	@make --no-print-directory help
	@exit 1
endif
ifndef MESOS_RELEASE
	@echo "Error: MESOS_RELEASE is undefined."
	@make --no-print-directory help
	@exit 1
endif

images: check-version
	docker build \
		--rm \
		--build-arg STORM_RELEASE=$(STORM_RELEASE) \
       	--build-arg MESOS_RELEASE=$(MESOS_RELEASE) \
       	--build-arg RELEASE=$(RELEASE) \
       	--build-arg JAVA_PRODUCT_VERSION=$(JAVA_PRODUCT_VERSION) \
       	--build-arg MIRROR=$(MIRROR) \
       	--build-arg STORM_URL=$(STORM_URL) \
		-t $(DOCKER_REPO)/storm:$(RELEASE)-$(STORM_RELEASE)-$(MESOS_RELEASE)-jdk$(JAVA_PRODUCT_VERSION) .
	docker build \
		--rm \
		-f onbuild/Dockerfile \
		-t $(DOCKER_REPO)/storm:$(RELEASE)-$(STORM_RELEASE)-$(MESOS_RELEASE)-jdk$(JAVA_PRODUCT_VERSION)-onbuild .

push: check-version
	docker push $(DOCKER_REPO)/storm:$(RELEASE)-$(STORM_RELEASE)-$(MESOS_RELEASE)-jdk$(JAVA_PRODUCT_VERSION)
	docker push $(DOCKER_REPO)/storm:$(RELEASE)-$(STORM_RELEASE)-$(MESOS_RELEASE)-jdk$(JAVA_PRODUCT_VERSION)-onbuild
