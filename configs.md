

# storm.yaml

## Nimbus
storm.yaml is bundled with mesos-storm image, and can be overridden for nimbus (mesos framework) by:
* extend the image (FROM mesos-storm), and ADD the storm.yaml of your choosing (ADD storm.yaml $STORM_HOME/conf/storm.yaml)
* launch the image with -c flags, e.g. -c storm.zookeeper.servers=[\"zk.hostname\"]

## Supervisor

For supervisor, the bundled storm.yaml will be used, AND any flags passed into nimus will also be overlaid.

TODO: supervisor storm.yaml 
Also an embedded jetty server should allow upload of an optional separate storm.yaml for use with supervisors created by the framework. 
This would allow adjustment of supervisor configs without redeploy of framework.

TODO: http deployment
This jetty server should also serve as a simple http based deployer, so that simple http clients can be used to install topologies (no requirement to use Nimbus thrift client)

