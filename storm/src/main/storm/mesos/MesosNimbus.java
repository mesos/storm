/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos;

import org.apache.storm.Config;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ContainerInfo;
import org.apache.mesos.Protos.Credential;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ReservationType;
import storm.mesos.resources.ResourceEntries.RangeResourceEntry;
import storm.mesos.resources.ResourceEntries.ScalarResourceEntry;
import storm.mesos.resources.ResourceEntry;
import storm.mesos.resources.ResourceNotAvailableException;
import storm.mesos.resources.ResourceType;
import storm.mesos.schedulers.StormSchedulerImpl;
import storm.mesos.schedulers.IMesosStormScheduler;
import storm.mesos.shims.CommandLineShimFactory;
import storm.mesos.shims.ICommandLineShim;
import storm.mesos.shims.LocalStateShim;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.ZKClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import static storm.mesos.util.PrettyProtobuf.offerIDListToString;
import static storm.mesos.util.PrettyProtobuf.offerToString;
import static storm.mesos.util.PrettyProtobuf.offerMapToString;
import static storm.mesos.util.PrettyProtobuf.taskInfoListToString;

public class MesosNimbus implements INimbus {
  public static final String CONF_EXECUTOR_URI = "mesos.executor.uri";
  public static final String CONF_MASTER_URL = "mesos.master.url";
  public static final String CONF_MASTER_FAILOVER_TIMEOUT_SECS = "mesos.master.failover.timeout.secs";
  public static final String CONF_MESOS_ALLOWED_HOSTS = "mesos.allowed.hosts";
  public static final String CONF_MESOS_DISALLOWED_HOSTS = "mesos.disallowed.hosts";
  public static final String CONF_MESOS_ROLE = "mesos.framework.role";
  public static final String CONF_MESOS_PRINCIPAL = "mesos.framework.principal";
  public static final String CONF_MESOS_SECRET_FILE = "mesos.framework.secret.file";

  public static final String CONF_MESOS_CHECKPOINT = "mesos.framework.checkpoint";
  public static final String CONF_MESOS_OFFER_FILTER_SECONDS = "mesos.offer.filter.seconds";
  public static final String CONF_MESOS_OFFER_EXPIRY_MULTIPLIER = "mesos.offer.expiry.multiplier";
  public static final String CONF_MESOS_LOCAL_FILE_SERVER_PORT = "mesos.local.file.server.port";
  public static final String CONF_MESOS_FRAMEWORK_NAME = "mesos.framework.name";
  public static final String CONF_MESOS_FRAMEWORK_USER = "mesos.framework.user";
  public static final String CONF_MESOS_PREFER_RESERVED_RESOURCES = "mesos.prefer.reserved.resources";
  public static final String CONF_MESOS_CONTAINER_DOCKER_IMAGE = "mesos.container.docker.image";
  public static final String CONF_MESOS_SUPERVISOR_STORM_LOCAL_DIR = "mesos.supervisor.storm.local.dir";
  public static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  public static final String DEFAULT_MESOS_COMPONENT_NAME_DELIMITER = "|";

  public static final String CONF_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";
  public static final String CONF_ZOOKEEPER_PORT = "storm.zookeeper.port";
  public static final String CONF_STORM_LOGVIEWER_ZK_DIR = "storm.logviewer.zookeeper.dir";

  public static final int TASK_RECONCILIATION_INTERVAL = 300000; // 5 minutes

  private static final Logger LOG = LoggerFactory.getLogger(MesosNimbus.class);
  private final Object _offersLock = new Object();
  protected java.net.URI _configUrl;
  private LocalStateShim _state;
  private NimbusMesosScheduler _mesosScheduler;
  private ZKClient _zkClient;
  private String _logviewerZkDir;
  private Timer _timer = new Timer();
  protected volatile SchedulerDriver _driver;
  private volatile boolean _registeredAndInitialized = false;
  private Map mesosStormConf;
  private Set<String> _allowedHosts;
  private Set<String> _disallowedHosts;
  private Optional<Integer> _localFileServerPort;
  private Map<OfferID, Offer> _offers;
  private LocalFileServer _httpServer;
  private IMesosStormScheduler _stormScheduler = null;

  private boolean _preferReservedResources = true;
  private Optional<String> _container = Optional.absent();
  private Path _generatedConfPath;

  private static Set listIntoSet(List l) {
    if (l == null) {
      return null;
    } else {
      return new HashSet<>(l);
    }
  }

  public MesosNimbus() {
    // This doesn't do anything since we can't make the scheduler until we've been registered
  }

  public static void main(String[] args) {
    org.apache.storm.daemon.nimbus.launch(new MesosNimbus());
  }

  private static String launchTaskListToString(List<LaunchTask> launchTasks) {
    StringBuilder sb = new StringBuilder(1024);
    for (LaunchTask launchTask : launchTasks) {
      sb.append("\n");
      sb.append(launchTask.toString());
    }
    return sb.toString();
  }

  @Override
  public IScheduler getForcedScheduler() {
    if (!_registeredAndInitialized) {
      // Since this scheduler hasn't been initialized, we will return null
      return null;
    }
    // TODO: Make it configurable. We should be able to specify the scheduler to use in the storm.yaml
    return (IScheduler) _stormScheduler;
  }

  @Override
  public String getHostName(Map<String, SupervisorDetails> map, String nodeId) {
    return nodeId;
  }

  @Override
  public void prepare(Map conf, String localDir) {
    try {
      initializeMesosStormConf(conf, localDir);
      startLocalHttpServer();

      MesosSchedulerDriver driver = createMesosDriver();

      driver.start();

      LOG.info("Waiting for scheduler driver to register MesosNimbus with mesos-master and complete initialization...");

      _mesosScheduler.waitUntilRegistered();

      LOG.info("Scheduler registration and initialization complete...");

      _registeredAndInitialized = true;

    } catch (Exception e) {
      LOG.error("Failed to prepare scheduler ", e);
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  void initializeMesosStormConf(Map conf, String localDir) {
    mesosStormConf = new HashMap();
    mesosStormConf.putAll(conf);

    try {
      _state = new LocalStateShim(localDir);
    } catch (IOException exp) {
      throw new RuntimeException(String.format("Encountered IOException while setting up LocalState at %s : %s", localDir, exp));
    }

    _allowedHosts = listIntoSet((List<String>) conf.get(CONF_MESOS_ALLOWED_HOSTS));
    _disallowedHosts = listIntoSet((List<String>) conf.get(CONF_MESOS_DISALLOWED_HOSTS));

    Set<String> zooKeeperServers = listIntoSet((List<String>) conf.get(CONF_ZOOKEEPER_SERVERS));
    String zooKeeperPort = String.valueOf(conf.get(CONF_ZOOKEEPER_PORT));
    _logviewerZkDir = Optional.fromNullable((String) conf.get(CONF_STORM_LOGVIEWER_ZK_DIR)).or("/logviewers");
    if (zooKeeperPort == null || zooKeeperServers == null) {
      LOG.error("ZooKeeper configs are not found in storm.yaml");
    } else {
      StringBuilder connectionString = new StringBuilder();
      for (String server : zooKeeperServers) {
        connectionString.append(String.format("%s:%s,", server, zooKeeperPort));
      }
      _zkClient = new ZKClient(connectionString.substring(0, connectionString.length() - 1));
      if (!_zkClient.nodeExists(_logviewerZkDir)) {
        _zkClient.createNode(_logviewerZkDir);
        LOG.info("Created general ZK directory for logviewer state at: {}", _logviewerZkDir);
      }
    }

    Boolean preferReservedResources = (Boolean) conf.get(CONF_MESOS_PREFER_RESERVED_RESOURCES);
    if (preferReservedResources != null) {
      _preferReservedResources = preferReservedResources;
    }

    _container = Optional.fromNullable((String) conf.get(CONF_MESOS_CONTAINER_DOCKER_IMAGE));
    _mesosScheduler = new NimbusMesosScheduler(this, _zkClient, _logviewerZkDir);

    // Generate YAML to be served up to clients
    _generatedConfPath = Paths.get(
        Optional.fromNullable((String) conf.get(Config.STORM_LOCAL_DIR)).or("./"),
        "generated-conf");
    if (!_generatedConfPath.toFile().exists() && !_generatedConfPath.toFile().mkdirs()) {
      throw new RuntimeException("Couldn't create generated-conf dir, _generatedConfPath=" + _generatedConfPath.toString());
    }

    try {
      mesosStormConf.put(Config.NIMBUS_HOST, MesosCommon.getNimbusHost(mesosStormConf));
    } catch (UnknownHostException exp) {
      throw new RuntimeException(String.format("Exception while configuring nimbus host: %s", exp));
    }

    Path pathToDumpConfig = Paths.get(_generatedConfPath.toString(), "storm.yaml");
    try {
      File generatedConf = pathToDumpConfig.toFile();
      Yaml yaml = new Yaml();
      FileWriter writer = new FileWriter(generatedConf);
      yaml.dump(mesosStormConf, writer);
    } catch (IOException exp) {
      LOG.error("Could not dump generated config to {}", pathToDumpConfig);
    }
  }

  @SuppressWarnings("unchecked")
  protected void startLocalHttpServer() throws Exception {
    createLocalServerPort();
    setupHttpServer();
  }

  public void doRegistration(final SchedulerDriver driver, Protos.FrameworkID id) {
    _driver = driver;
    // Now that we've set the driver, we can create our scheduler
    _stormScheduler = new StormSchedulerImpl(_driver);

    _timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        // performing "implicit" reconciliation; master will respond with the latest state for all currently known
        // non-terminal tasks
        Collection<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
        _driver.reconcileTasks(taskStatuses);
        LOG.info("Performing tasking reconciliation between scheduler and master");
      }
    }, TASK_RECONCILIATION_INTERVAL, TASK_RECONCILIATION_INTERVAL); // reconciliation performed every 5 minutes
    _state.put(FRAMEWORK_ID, id.getValue());
    _offers = new HashMap<Protos.OfferID, Protos.Offer>();
  }

  public void shutdown() throws Exception {
    _httpServer.shutdown();
  }

  public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
    synchronized (_offersLock) {
      if (_offers == null) {
        return;
      }
      LOG.debug("resourceOffers: Currently have {} offers buffered {}",
                _offers.size(), (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));

      for (Protos.Offer offer : offers) {
        if (isHostAccepted(offer.getHostname())) {
          // TODO(ksoundararaj): Should we record the following as info instead of debug
          LOG.info("resourceOffers: Recording offer: {}", offerToString(offer));
          _offers.put(offer.getId(), offer);
        } else {
          LOG.info("resourceOffers: Declining offer: {}", offerToString(offer));
          driver.declineOffer(offer.getId());
        }
      }
      LOG.info("resourceOffers: After processing offers, now have {} offers buffered: {}",
                _offers.size(), offerMapToString(_offers));
    }
  }

  public void offerRescinded(OfferID id) {
    synchronized (_offersLock) {
      _offers.remove(id);
    }
  }

  private void createLocalServerPort() {
    Integer port = (Integer) mesosStormConf.get(CONF_MESOS_LOCAL_FILE_SERVER_PORT);
    LOG.debug("LocalFileServer configured to listen on port: {}", port);
    _localFileServerPort = Optional.fromNullable(port);
  }

  private void setupHttpServer() throws Exception {
    _httpServer = new LocalFileServer();
    _configUrl = _httpServer.serveDir("/generated-conf", _generatedConfPath.toString(), MesosCommon.getNimbusHost(mesosStormConf), _localFileServerPort);

    LOG.info("Started HTTP server from which config for the MesosSupervisor's may be fetched. URL: {}", _configUrl);
  }

  private MesosSchedulerDriver createMesosDriver() throws IOException {
    MesosSchedulerDriver driver;
    Credential credential;
    FrameworkInfo.Builder finfo = createFrameworkBuilder();
    LOG.info(String.format("Registering framework with role '%s'", finfo.getRole()));

    if ((credential = getCredential(finfo)) != null) {
      driver = new MesosSchedulerDriver(_mesosScheduler, finfo.build(), (String) mesosStormConf.get(CONF_MASTER_URL), credential);
    } else {
      driver = new MesosSchedulerDriver(_mesosScheduler, finfo.build(), (String) mesosStormConf.get(CONF_MASTER_URL));
    }

    return driver;
  }

  public boolean isHostAccepted(String hostname) {
    return
        (_allowedHosts == null && _disallowedHosts == null) ||
            (_allowedHosts != null && _allowedHosts.contains(hostname)) ||
            (_disallowedHosts != null && !_disallowedHosts.contains(hostname));
  }


  @Override
  public Collection<WorkerSlot> allSlotsAvailableForScheduling(
          Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
    if (!_registeredAndInitialized) {
      return new ArrayList<WorkerSlot>();
    }
    synchronized (_offersLock) {
      if (!_container.isPresent()) {
        launchLogviewer(existingSupervisors);
      }
      return _stormScheduler.allSlotsAvailableForScheduling(
              _offers,
              existingSupervisors,
              topologies,
              topologiesMissingAssignments);
    }
  }

  private void launchLogviewer(Collection<SupervisorDetails> existingSupervisors) {
    final double logviewerCpu = 0.5;
    final double logviewerMem = 400.0;

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(_offers);

    String supervisorStormLocalDir = getStormLocalDirForWorkers();
    String configUri = getFullConfigUri();
    String logviewerCommand = String.format(
                    "cp storm.yaml storm-mesos*/conf" +
                    " && cd storm-mesos*" +
                    " && python bin/storm logviewer" +
                    " -c storm.local.dir=%s", supervisorStormLocalDir);

    /**
     *  Go through each existing supervisor and:
     *    1. Check ZK if logviewer already exists on worker host, if not then continue
     *    2. Look in the aggregratedOffersPerNode and check the node for offers, if there are offers then continue
     *    3. Create a TaskInfo for the logviewer corresponding to the correct Offers and launch the task
     *    4. Update ZK to reflect the existence of new logviewer on worker host
     */

    for (SupervisorDetails supervisor : existingSupervisors) {
      List<TaskInfo> logviewerTask = new ArrayList<TaskInfo>();
      LOG.info("launchLogviewer: Supervisor ID: {}", supervisor.getId());

      String nodeId = supervisor.getId().split("\\" + DEFAULT_MESOS_COMPONENT_NAME_DELIMITER)[0];

      if (_zkClient.nodeExists(String.format("%s/%s", _logviewerZkDir, nodeId))) {
        LOG.info("launchLogviewer: Logviewer already exists on this host: {}", nodeId);
        continue;
      }

      AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(nodeId);
      if (aggregatedOffers == null) {
        LOG.info("launchLogviewer: No offers for this host: {}", nodeId);
        continue;
      }

      List<OfferID> offerIDList = aggregatedOffers.getOfferIDList();

      List<Resource> resources = new ArrayList<Resource>();
      List<ResourceEntry> resourceEntryList = null;

      try {
        resourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.CPU, new ScalarResourceEntry(logviewerCpu));
        resources.addAll(createMesosScalarResourceList(ResourceType.CPU, resourceEntryList));
        resourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.MEM, new ScalarResourceEntry(logviewerMem));
        resources.addAll(createMesosScalarResourceList(ResourceType.MEM, resourceEntryList));
      } catch (ResourceNotAvailableException e) {
        LOG.warn("launchLogviewer: Unable to launch logviewer because some resources were unavailable. Available aggregatedOffers: %s", aggregatedOffers);
        continue;
      }

      CommandInfo.Builder commandInfoBuilder = CommandInfo.newBuilder()
              .addUris(URI.newBuilder().setValue((String) mesosStormConf.get(CONF_EXECUTOR_URI)))
              .addUris(URI.newBuilder().setValue(configUri))
              .setValue(logviewerCommand);

      TaskID taskId = TaskID.newBuilder()
              .setValue(String.format("%s%slogviewer",  nodeId, DEFAULT_MESOS_COMPONENT_NAME_DELIMITER))
              .build();

      TaskInfo task = TaskInfo.newBuilder()
              .setTaskId(taskId)
              .setName("logviewer")
              .setSlaveId(aggregatedOffers.getSlaveID())
              .setCommand(commandInfoBuilder.build())
              .addAllResources(resources)
              .build();

      logviewerTask.add(task);

      LOG.info("launchLogviewer: Using offerIDs: {} on host: {} to launch logviewer", offerIDListToString(offerIDList), nodeId);

      _driver.launchTasks(offerIDList, logviewerTask);
      for (OfferID offerID : offerIDList) {
        _offers.remove(offerID);
      }

      String logviewerZKPath = String.format("%s/%s", _logviewerZkDir, nodeId);
      _zkClient.createNode(logviewerZKPath);
      LOG.info("launchLogviewer: Create logviewer state in zk: {}", logviewerZKPath);
    }
  }

  /**
   *  This method is invoked after IScheduler.schedule assigns the worker slots to the topologies that need assignments
   *
   *  @param topologies                             - Information about all topologies
   *  @param slotsForTopologiesNeedingAssignments   - A map of topology name and collection of worker slots that are assigned to the topologies
   *                                                  that need assignments
   */
  @Override
  public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slotsForTopologiesNeedingAssignments) {
    if (slotsForTopologiesNeedingAssignments.isEmpty()) {
      LOG.info("assignSlots: no slots passed in, nothing to do");
      return;
    }

    // This is purely to print the debug information. Otherwise, the following for loop is unnecessary.
    for (Map.Entry<String, Collection<WorkerSlot>> topologyToSlots : slotsForTopologiesNeedingAssignments.entrySet()) {
      String topologyId = topologyToSlots.getKey();
      List<String> topologySlotAssignmentStrings = new ArrayList<String>();
      String info = "assignSlots: " + topologyId + " being assigned to " + topologyToSlots.getValue().size() + " slots (worker:port, cpu, mem) as follows: ";
      for (WorkerSlot slot : topologyToSlots.getValue()) {
        TopologyDetails details = topologies.getById(topologyId);
        topologySlotAssignmentStrings.add("(" + slot + ", " + MesosCommon.topologyWorkerCpu(mesosStormConf, details) + ", " + MesosCommon.topologyWorkerMem(mesosStormConf, details) + ")");
      }
      if (!topologyToSlots.getValue().isEmpty()) {
        info += StringUtils.join(topologySlotAssignmentStrings, ", ");
        LOG.info(info);
      }
    }

    synchronized (_offersLock) {
      /**
       * We need to call getAggregatedOffersPerNode again here for the following reasons
       *   1. Because _offers could have changed between `allSlotsAvailableForScheduling()' and `assignSlots()'
       *   2. In `allSlotsAvailableForScheduling()', we change what is returned by `getAggregatedOffersPerNode'
       *      in order to calculate the number of slots available.
       */
      Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(_offers);
      Map<String, List<TaskInfo>> tasksToLaunchPerNode = getTasksToLaunch(topologies, slotsForTopologiesNeedingAssignments, aggregatedOffersPerNode);

      for (String node : tasksToLaunchPerNode.keySet()) {
        List<OfferID> offerIDList = aggregatedOffersPerNode.get(node).getOfferIDList();
        List<TaskInfo> taskInfoList = tasksToLaunchPerNode.get(node);

        LOG.info("Using offerIDs: {} on host: {} to launch tasks: {}", offerIDListToString(offerIDList), node, taskInfoListToString(taskInfoList));

        _driver.launchTasks(offerIDList, taskInfoList);
        for (OfferID offerID: offerIDList) {
          _offers.remove(offerID);
        }
      }
    }
  }

  // Set the MesosSupervisor's storm.local.dir value. By default it is set to "storm-local",
  // which is the same as the storm-core default, which is interpreted relative to the pwd of
  // the storm daemon (the mesos executor sandbox in the case of the MesosSupervisor).
  //
  // If this isn't done, then MesosSupervisor inherits MesosNimbus's storm.local.dir config.
  // That is fine if the MesosNimbus storm.local.dir config is just the default too ("storm-local"),
  // since the mesos executor sandbox is isolated per-topology.
  // However, if the MesosNimbus storm.local.dir is specialized (e.g., /var/run/storm-local), then
  // this will cause problems since multiple topologies on the same host would use the same
  // storm.local.dir and thus interfere with each other's state. And *that* leads to one topology's
  // supervisor killing the workers of every other topology on the same host (due to interference
  // in the stored worker assignments).
  // Note that you *can* force the MesosSupervisor to use a specific directory by setting
  // the "mesos.supervisor.storm.local.dir" variable in the MesosNimbus's storm.yaml.
  public String getStormLocalDirForWorkers() {
    String supervisorStormLocalDir = (String) mesosStormConf.get(CONF_MESOS_SUPERVISOR_STORM_LOCAL_DIR);
    if (supervisorStormLocalDir == null) {
      supervisorStormLocalDir = MesosCommon.DEFAULT_SUPERVISOR_STORM_LOCAL_DIR;
    }
    return supervisorStormLocalDir;
  }

  private Resource createMesosScalarResource(ResourceType resourceType, ScalarResourceEntry scalarResourceEntry) {
    return Resource.newBuilder()
                   .setName(resourceType.toString())
                   .setType(Protos.Value.Type.SCALAR)
                   .setScalar(Scalar.newBuilder().setValue(scalarResourceEntry.getValue()))
                   .build();
  }

  private List<Resource> createMesosScalarResourceList(ResourceType resourceType, List<ResourceEntry> scalarResourceEntryList) {
    List<Resource> retVal = new ArrayList<>();
    ScalarResourceEntry scalarResourceEntry = null;

    for (ResourceEntry resourceEntry : scalarResourceEntryList) {
      scalarResourceEntry = (ScalarResourceEntry) resourceEntry;
      Resource.Builder resourceBuilder = Resource.newBuilder()
                                                 .setName(resourceType.toString())
                                                 .setType(Protos.Value.Type.SCALAR)
                                                 .setScalar(Scalar.newBuilder().setValue(scalarResourceEntry.getValue()));
      if (resourceEntry.getReservationType() != null && resourceEntry.getReservationType().equals(ReservationType.STATIC)) {
        resourceBuilder.setRole(MesosCommon.getRole(mesosStormConf));
      }
      retVal.add(resourceBuilder.build());
    }
    return retVal;
  }

  private Resource createMesosRangeResource(ResourceType resourceType, RangeResourceEntry rangeResourceEntry) {
    Ranges.Builder rangesBuilder = Ranges.newBuilder();
    Range rangeBuilder = Range.newBuilder()
                              .setBegin(rangeResourceEntry.getBegin())
                              .setEnd(rangeResourceEntry.getEnd()).build();
    rangesBuilder.addRange(rangeBuilder);

    Resource.Builder resourceBuilder = Resource.newBuilder()
                                               .setName(resourceType.toString())
                                               .setType(Protos.Value.Type.RANGES)
                                               .setRanges(rangesBuilder.build());
    if (rangeResourceEntry.getReservationType() != null && rangeResourceEntry.getReservationType().equals(ReservationType.STATIC)) {
      resourceBuilder.setRole(MesosCommon.getRole(mesosStormConf));
    }
    return resourceBuilder.build();
  }

  String getFullConfigUri() {
    try {
      return new URL(_configUrl.toURL(),
                 _configUrl.getPath() + "/storm.yaml").toString();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  private ExecutorInfo.Builder getExecutorInfoBuilder(TopologyDetails details, String executorDataStr,
                                                      String executorName,
                                                      List<Resource> executorResources,
                                                      String extraConfig) {
    String configUri;

    configUri = getFullConfigUri();

    ExecutorInfo.Builder executorInfoBuilder = ExecutorInfo.newBuilder();

    executorInfoBuilder
      .setName(executorName)
      .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
      .setData(ByteString.copyFromUtf8(executorDataStr))
      .addAllResources(executorResources);

    ICommandLineShim commandLineShim = CommandLineShimFactory.makeCommandLineShim(_container.isPresent(), extraConfig);
    /**
     *  _container.isPresent() might be slightly misleading at first blush. It is only checking whether or not
     *  CONF_MESOS_CONTAINER_DOCKER_IMAGE is set to a value other than null.
     */
    if (_container.isPresent()) {
      executorInfoBuilder.setCommand(CommandInfo.newBuilder()
                                                .addUris(URI.newBuilder().setValue(configUri))
                                                .setValue(commandLineShim.getCommandLine(details.getId())))
                         .setContainer(ContainerInfo.newBuilder()
                                                    .setType(ContainerInfo.Type.DOCKER)
                                                    .setDocker(ContainerInfo.DockerInfo.newBuilder()
                                                                                       .setImage(_container.get())
                                                                                       .setNetwork(ContainerInfo.DockerInfo.Network.HOST)
                                                                                       .setForcePullImage(true)
                                                                                       .build()
                                                    ).build());
    } else {
      executorInfoBuilder.setCommand(CommandInfo.newBuilder()
                                                .addUris(URI.newBuilder().setValue((String) mesosStormConf.get(CONF_EXECUTOR_URI)))
                                                .addUris(URI.newBuilder().setValue(configUri))
                                                .setValue(commandLineShim.getCommandLine(details.getId())));
    }

    return executorInfoBuilder;
  }


  public Map<String, List<TaskInfo>> getTasksToLaunch(Topologies topologies,
                                                      Map<String, Collection<WorkerSlot>> slots,
                                                      Map<String, AggregatedOffers> aggregatedOffersPerNode) {
    Map<String, List<TaskInfo>> tasksToLaunchPerNode = new HashMap<>();

    for (String topologyId : slots.keySet()) {
      Collection<WorkerSlot> slotList = slots.get(topologyId);
      TopologyDetails topologyDetails = topologies.getById(topologyId);
      Set<String> hostsWithSupervisors = new HashSet<>();

      double executorCpu = MesosCommon.executorCpu(mesosStormConf);
      double executorMem = MesosCommon.executorMem(mesosStormConf);
      double workerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
      double workerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

      for (WorkerSlot slot : slotList) {
        // for this task we start with the assumption that we only need the worker resources, we'll add the executor resources later if needed.
        double requiredCpu = workerCpu;
        double requiredMem = workerMem;

        String workerHost = slot.getNodeId();
        Long workerPort = Long.valueOf(slot.getPort());

        AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(slot.getNodeId());
        String workerPrefix = "";
        if (mesosStormConf.containsKey(MesosCommon.WORKER_NAME_PREFIX)) {
          workerPrefix = MesosCommon.getWorkerPrefix(mesosStormConf, topologyDetails);
        }

        // Account for executor resources only the first time we see this host for this topology
        if (!hostsWithSupervisors.contains(workerHost)) {
          requiredCpu += executorCpu;
          requiredMem += executorMem;
        }

        Map executorData = new HashMap();
        executorData.put(MesosCommon.SUPERVISOR_ID, MesosCommon.supervisorId(slot.getNodeId(), topologyDetails.getId()));
        executorData.put(MesosCommon.ASSIGNMENT_ID, workerPrefix + slot.getNodeId());

        String topologyAndNodeId = topologyDetails.getId() + " | " + slot.getNodeId();
        String executorName = "storm-supervisor | " + topologyAndNodeId;
        String taskName = "storm-worker | " + topologyAndNodeId + ":" + slot.getPort();
        String executorDataStr = JSONValue.toJSONString(executorData);
        String extraConfig = "";

        if (!aggregatedOffers.isFit(mesosStormConf, topologyDetails, workerPort, hostsWithSupervisors.contains(workerHost))) {
          LOG.error(String.format("Unable to launch worker %s for topology %s. Required cpu: %f, Required mem: %f, Required port: %d. Available aggregatedOffers : %s",
                                  workerHost, topologyDetails.getId(), requiredCpu, requiredMem, workerPort, aggregatedOffers));
          continue;
        }

        List<Resource> executorResources = new ArrayList<>();
        List<Resource> workerResources = new ArrayList<>();

        try {
          // This list will hold CPU and MEM resources
          List<ResourceEntry> scalarResourceEntryList = null;
          // This list will hold PORTS resources
          List<ResourceEntry> rangeResourceEntryList = null;

          if (hostsWithSupervisors.contains(workerHost)) {
            // Since we already have a supervisor on this host, we don't need to account for its resources by obtaining them from
            // an offer, but we do need to ensure that executor resources are the same for every task, or mesos rejects the task
            executorResources.add(createMesosScalarResource(ResourceType.CPU, new ScalarResourceEntry(executorCpu)));
            executorResources.add(createMesosScalarResource(ResourceType.MEM, new ScalarResourceEntry(executorMem)));
          } else {
            // Need to account for executor resources, since this might be the first executor on this host for this topology
            // (we have no way to tell this without some state reconciliation or storage).
            scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.CPU, new ScalarResourceEntry(executorCpu));
            executorResources.addAll(createMesosScalarResourceList(ResourceType.CPU, scalarResourceEntryList));
            scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.MEM, new ScalarResourceEntry(executorMem));
            executorResources.addAll(createMesosScalarResourceList(ResourceType.MEM, scalarResourceEntryList));
          }

          String supervisorStormLocalDir = getStormLocalDirForWorkers();
          extraConfig += String.format(" -c storm.local.dir=%s", supervisorStormLocalDir);

          // First add to the list of required worker resources the CPU resources we were able to secure
          scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.CPU, new ScalarResourceEntry(workerCpu));
          workerResources.addAll(createMesosScalarResourceList(ResourceType.CPU, scalarResourceEntryList));
          // Then add to the list of required worker resources the MEM resources we were able to secure
          scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.MEM, new ScalarResourceEntry(workerMem));
          workerResources.addAll(createMesosScalarResourceList(ResourceType.MEM, scalarResourceEntryList));
          // Finally add to the list of required worker resources the PORTS resources we were able to secure (which we expect to be a single port)
          rangeResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.PORTS, new RangeResourceEntry(workerPort, workerPort));
          for (ResourceEntry resourceEntry : rangeResourceEntryList) {
            workerResources.add(createMesosRangeResource(ResourceType.PORTS, (RangeResourceEntry) resourceEntry));
          }
        } catch (ResourceNotAvailableException rexp) {
          LOG.warn("Unable to launch worker %s because some resources were unavailable. Required cpu: %f, Required mem: %f, Required port: %d. Available aggregatedOffers : %s",
                   workerHost, requiredCpu, requiredMem, workerPort, aggregatedOffers);
          continue;
        }

        hostsWithSupervisors.add(workerHost);

        ExecutorInfo.Builder executorInfoBuilder = getExecutorInfoBuilder(topologyDetails, executorDataStr, executorName, executorResources, extraConfig);
        TaskID taskId = TaskID.newBuilder()
                              .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort()))
                              .build();

        TaskInfo task = TaskInfo.newBuilder()
                                .setTaskId(taskId)
                                .setName(taskName)
                                .setSlaveId(aggregatedOffers.getSlaveID())
                                .setExecutor(executorInfoBuilder.build())
                                .addAllResources(workerResources)
                                .build();

        List<TaskInfo> taskInfoList = tasksToLaunchPerNode.get(slot.getNodeId());
        if (taskInfoList == null) {
          taskInfoList = new ArrayList<>();
          tasksToLaunchPerNode.put(slot.getNodeId(), taskInfoList);
        }
        taskInfoList.add(task);
      }
    }

    return tasksToLaunchPerNode;
  }

  private FrameworkInfo.Builder createFrameworkBuilder() throws IOException {
    Number failoverTimeout = Optional.fromNullable((Number) mesosStormConf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS)).or(24 * 7 * 3600);
    String role = Optional.fromNullable((String) mesosStormConf.get(CONF_MESOS_ROLE)).or("*");
    Boolean checkpoint = Optional.fromNullable((Boolean) mesosStormConf.get(CONF_MESOS_CHECKPOINT)).or(false);
    String frameworkName = Optional.fromNullable((String) mesosStormConf.get(CONF_MESOS_FRAMEWORK_NAME)).or("Storm!!!");
    String frameworkUser = Optional.fromNullable((String) mesosStormConf.get(CONF_MESOS_FRAMEWORK_USER)).or("");

    FrameworkInfo.Builder finfo = FrameworkInfo.newBuilder()
        .setName(frameworkName)
        .setFailoverTimeout(failoverTimeout.doubleValue())
        .setUser(frameworkUser)
        .setRole(role)
        .setCheckpoint(checkpoint);

    String id = _state.get(FRAMEWORK_ID);

    if (id != null) {
      finfo.setId(FrameworkID.newBuilder().setValue(id).build());
    }

    return finfo;
  }
  // Super ugly method but it only gets called once.
  private Method getCorrectSetSecretMethod(Class clazz) {
    try {
      return clazz.getMethod("setSecretBytes", ByteString.class);
    } catch (NoSuchMethodException e) {
      try {
        return clazz.getMethod("setSecret", ByteString.class);
      } catch (NoSuchMethodException e2) {
        // maybe overkill and we should just return a null but if we're passing auth
        // we probably want this versus an auth error if the contracts change in say 0.27.0
        throw new RuntimeException("Unable to find a setSecret method!", e2);
      }
    }
  }

  private Credential getCredential(FrameworkInfo.Builder finfo) {
    LOG.info("Checking for mesos authentication");

    Credential credential = null;

    String principal = Optional.fromNullable((String) mesosStormConf.get(CONF_MESOS_PRINCIPAL)).orNull();
    String secretFilename = Optional.fromNullable((String) mesosStormConf.get(CONF_MESOS_SECRET_FILE)).orNull();

    if (principal != null) {
      finfo.setPrincipal(principal);
      Credential.Builder credentialBuilder = Credential.newBuilder();
      credentialBuilder.setPrincipal(principal);
      if (StringUtils.isNotEmpty(secretFilename)) {
        try {
          // The secret cannot have a NewLine after it
          ByteString secret = ByteString.readFrom(new FileInputStream(secretFilename));
          Method setSecret = getCorrectSetSecretMethod(credentialBuilder.getClass());
          setSecret.invoke(credentialBuilder, secret);
        } catch (FileNotFoundException ex) {
          LOG.error("Mesos authentication secret file was not found", ex);
          throw new RuntimeException(ex);
        } catch (IOException ex) {
          LOG.error("Error reading Mesos authentication secret file", ex);
          throw new RuntimeException(ex);
        } catch (InvocationTargetException ex) {
          LOG.error("Reflection Error", ex);
          throw new RuntimeException(ex);
        } catch (IllegalAccessException ex) {
          LOG.error("Reflection Error", ex);
          throw new RuntimeException(ex);
        }
      }
      credential = credentialBuilder.build();
    }
    return credential;
  }
}
