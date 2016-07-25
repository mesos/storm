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

import backtype.storm.Config;
import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
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
import storm.mesos.schedulers.DefaultScheduler;
import storm.mesos.schedulers.IMesosStormScheduler;
import storm.mesos.shims.CommandLineShimFactory;
import storm.mesos.shims.ICommandLineShim;
import storm.mesos.shims.LocalStateShim;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static storm.mesos.util.PrettyProtobuf.offerIDListToString;
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
  public static final String CONF_MESOS_OFFER_LRU_CACHE_SIZE = "mesos.offer.lru.cache.size";
  public static final String CONF_MESOS_OFFER_FILTER_SECONDS = "mesos.offer.filter.seconds";
  public static final String CONF_MESOS_OFFER_EXPIRY_MULTIPLIER = "mesos.offer.expiry.multiplier";
  public static final String CONF_MESOS_LOCAL_FILE_SERVER_PORT = "mesos.local.file.server.port";
  public static final String CONF_MESOS_FRAMEWORK_NAME = "mesos.framework.name";
  public static final String CONF_MESOS_PREFER_RESERVED_RESOURCES = "mesos.prefer.reserved.resources";
  public static final String CONF_MESOS_CONTAINER_DOCKER_IMAGE = "mesos.container.docker.image";
  public static final String CONF_MESOS_SUPERVISOR_STORM_LOCAL_DIR = "mesos.supervisor.storm.local.dir";
  public static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  private static final Logger LOG = LoggerFactory.getLogger(MesosNimbus.class);
  private final Object _offersLock = new Object();
  protected java.net.URI _configUrl;
  private LocalStateShim _state;
  private NimbusScheduler _scheduler;
  volatile SchedulerDriver _driver;
  private Timer _timer = new Timer();
  private Map mesosStormConf;
  private Set<String> _allowedHosts;
  private Set<String> _disallowedHosts;
  private Optional<Integer> _localFileServerPort;
  private RotatingMap<OfferID, Offer> _offers;
  private LocalFileServer _httpServer;
  private Map<TaskID, Offer> taskIDtoOfferMap;
  private ScheduledExecutorService timerScheduler =
      Executors.newScheduledThreadPool(1);
  private IMesosStormScheduler _mesosStormScheduler = null;

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
    this._mesosStormScheduler = new DefaultScheduler();
  }

  public static void main(String[] args) {
    backtype.storm.daemon.nimbus.launch(new MesosNimbus());
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
    // TODO: Make it configurable. We should be able to specify the scheduler to use in the storm.yaml
    return (IScheduler) _mesosStormScheduler;
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

      _scheduler.waitUntilRegistered();

      LOG.info("Scheduler registration and initialization complete...");

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
    Boolean preferReservedResources = (Boolean) conf.get(CONF_MESOS_PREFER_RESERVED_RESOURCES);
    if (preferReservedResources != null) {
      _preferReservedResources = preferReservedResources;
    }

    _container = Optional.fromNullable((String) conf.get(CONF_MESOS_CONTAINER_DOCKER_IMAGE));
    _scheduler = new NimbusScheduler(this);

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
    _state.put(FRAMEWORK_ID, id.getValue());
    Number filterSeconds = Optional.fromNullable((Number) mesosStormConf.get(CONF_MESOS_OFFER_FILTER_SECONDS)).or(120);
    final Protos.Filters filters = Protos.Filters.newBuilder()
        .setRefuseSeconds(filterSeconds.intValue())
        .build();
    _offers = new RotatingMap<>(
        new RotatingMap.ExpiredCallback<Protos.OfferID, Protos.Offer>() {
          @Override
          public void expire(Protos.OfferID key, Protos.Offer val) {
            driver.declineOffer(
                val.getId(),
                filters
            );
          }
        }
    );

    Number lruCacheSize = Optional.fromNullable((Number) mesosStormConf.get(CONF_MESOS_OFFER_LRU_CACHE_SIZE)).or(1000);
    final int intLruCacheSize = lruCacheSize.intValue();
    taskIDtoOfferMap = Collections.synchronizedMap(new LinkedHashMap<Protos.TaskID, Protos.Offer>(intLruCacheSize + 1, .75F, true) {
      // This method is called just after a new entry has been added
      public boolean removeEldestEntry(Map.Entry eldest) {
        return size() > intLruCacheSize;
      }
    });

    Number offerExpired = Optional.fromNullable((Number) mesosStormConf.get(Config.NIMBUS_MONITOR_FREQ_SECS)).or(10);
    Number expiryMultiplier = Optional.fromNullable((Number) mesosStormConf.get(CONF_MESOS_OFFER_EXPIRY_MULTIPLIER)).or(2.5);
    _timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        try {
          synchronized (_offersLock) {
            _offers.rotate();
          }
        } catch (Throwable t) {
          LOG.error("Received fatal error Halting process...", t);
          Runtime.getRuntime().halt(2);
        }
      }
    }, 0, Math.round(1000 * expiryMultiplier.doubleValue() * offerExpired.intValue()));
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
          LOG.debug("resourceOffers: Recording offer from host: {}, offerId: {}",
                    offer.getHostname(), offer.getId().getValue());
          _offers.put(offer.getId(), offer);
        } else {
          LOG.debug("resourceOffers: Declining offer from host: {}, offerId: {}",
                    offer.getHostname(), offer.getId().getValue());
          driver.declineOffer(offer.getId());
        }
      }
      LOG.debug("resourceOffers: After processing offers, now have {} offers buffered: {}",
                _offers.size(), offerMapToString(_offers));
    }
  }

  public void offerRescinded(OfferID id) {
    synchronized (_offersLock) {
      _offers.remove(id);
    }
  }

  public void taskTerminated(final TaskID taskId) {
    timerScheduler.schedule(new Runnable() {
      @Override
      public void run() {
        taskIDtoOfferMap.remove(taskId);
      }
    }, MesosCommon.getSuicideTimeout(mesosStormConf), TimeUnit.SECONDS);
  }

  private void createLocalServerPort() {
    Integer port = (Integer) mesosStormConf.get(CONF_MESOS_LOCAL_FILE_SERVER_PORT);
    LOG.debug("LocalFileServer configured to listen on port: {}", port);
    _localFileServerPort = Optional.fromNullable(port);
  }

  private void setupHttpServer() throws Exception {
    _httpServer = new LocalFileServer();
    _configUrl = _httpServer.serveDir("/generated-conf", _generatedConfPath.toString(), _localFileServerPort);

    LOG.info("Started HTTP server from which config for the MesosSupervisor's may be fetched. URL: {}", _configUrl);
  }

  private MesosSchedulerDriver createMesosDriver() throws IOException {
    MesosSchedulerDriver driver;
    Credential credential;
    FrameworkInfo.Builder finfo = createFrameworkBuilder();
    LOG.info(String.format("Registering framework with role '%s'", finfo.getRole()));

    if ((credential = getCredential(finfo)) != null) {
      driver = new MesosSchedulerDriver(_scheduler, finfo.build(), (String) mesosStormConf.get(CONF_MASTER_URL), credential);
    } else {
      driver = new MesosSchedulerDriver(_scheduler, finfo.build(), (String) mesosStormConf.get(CONF_MASTER_URL));
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
    synchronized (_offersLock) {
      return _mesosStormScheduler.allSlotsAvailableForScheduling(
              _offers,
              existingSupervisors,
              topologies,
              topologiesMissingAssignments);
    }
  }

  private String getLogViewerConfig() {
    return String.format(" -c %s=true", MesosCommon.AUTO_START_LOGVIEWER_CONF);
  }

  /**
   *  This method is invoked after IScheduler.schedule assigns the worker slots to the topologies that need assignments
   *
   *  @param topologies                             - Information about all topologies
   *  @param slotsForTopologiesNeedingAssignments   - A map of topology name and collection of worker slots that are assigned to the topology
   *                                                  that need assignments
   */
  @Override
  public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slotsForTopologiesNeedingAssignments) {
    if (slotsForTopologiesNeedingAssignments.isEmpty()) {
      LOG.debug("assignSlots: no slots passed in, nothing to do");
      return;
    }

    // This is purely to print the debug information. Otherwise, the following for loop is unnecessary.
    for (Map.Entry<String, Collection<WorkerSlot>> topologyToSlots : slotsForTopologiesNeedingAssignments.entrySet()) {
      String topologyId = topologyToSlots.getKey();
      for (WorkerSlot slot : topologyToSlots.getValue()) {
        TopologyDetails details = topologies.getById(topologyId);
        LOG.debug("assignSlots: topologyId: {} worker being assigned to slot: {} with workerCpu: {} workerMem: {}",
                  topologyId, slot, MesosCommon.topologyWorkerCpu(mesosStormConf, details), MesosCommon.topologyWorkerMem(mesosStormConf, details));
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

        LOG.info("Using offerIDs: " + offerIDListToString(offerIDList) + " on host: " + node + " to launch tasks: " + taskInfoListToString(taskInfoList));

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
      if (resourceEntry.getReservationType() != null && resourceEntry.getReservationType().equals(ReservationType.STATICALLY_RESERVED)) {
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
    if (rangeResourceEntry.getReservationType() != null && rangeResourceEntry.getReservationType().equals(ReservationType.STATICALLY_RESERVED)) {
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

      double workerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
      double workerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);
      double executorCpu = MesosCommon.executorCpu(mesosStormConf);
      double executorMem = MesosCommon.executorMem(mesosStormConf);

      for (WorkerSlot slot : slotList) {
        double requiredCpu = workerCpu;
        double requiredMem = workerMem;

        String workerHost = slot.getNodeId();
        Long workerPort = Long.valueOf(slot.getPort());

        AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(slot.getNodeId());
        String workerPrefix = "";
        if (mesosStormConf.containsKey(MesosCommon.WORKER_NAME_PREFIX)) {
          workerPrefix = MesosCommon.getWorkerPrefix(mesosStormConf, topologyDetails);
        }

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
          LOG.error(String.format("Unable to launch worker %s. Required cpu: %f, Required mem: %f. Available aggregatedOffers : %s",
                                  workerHost, requiredCpu, requiredMem, aggregatedOffers));
          continue;
        }

        List<Resource> executorResources = new ArrayList<>();
        List<Resource> workerResources = new ArrayList<>();

        try {
          List<ResourceEntry> scalarResourceEntryList = null;
          List<ResourceEntry> rangeResourceEntryList = null;

          if (hostsWithSupervisors.contains(workerHost)) {
            executorResources.add(createMesosScalarResource(ResourceType.CPU, new ScalarResourceEntry(executorCpu)));
            executorResources.add(createMesosScalarResource(ResourceType.MEM, new ScalarResourceEntry(executorMem)));
          } else {
            scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.CPU, new ScalarResourceEntry(executorCpu));
            executorResources.addAll(createMesosScalarResourceList(ResourceType.CPU, scalarResourceEntryList));
            scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.MEM, new ScalarResourceEntry(executorMem));
            executorResources.addAll(createMesosScalarResourceList(ResourceType.MEM, scalarResourceEntryList));
          }

          String supervisorStormLocalDir = getStormLocalDirForWorkers();
          extraConfig += String.format(" -c storm.local.dir=%s", supervisorStormLocalDir);

          scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.CPU, new ScalarResourceEntry(workerCpu));
          workerResources.addAll(createMesosScalarResourceList(ResourceType.CPU, scalarResourceEntryList));
          scalarResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.MEM, new ScalarResourceEntry(workerMem));
          workerResources.addAll(createMesosScalarResourceList(ResourceType.MEM, scalarResourceEntryList));
          rangeResourceEntryList = aggregatedOffers.reserveAndGet(ResourceType.PORTS, new RangeResourceEntry(workerPort, workerPort));
          for (ResourceEntry resourceEntry : rangeResourceEntryList) {
            workerResources.add(createMesosRangeResource(ResourceType.PORTS, (RangeResourceEntry) resourceEntry));
          }
        } catch (ResourceNotAvailableException rexp) {
          LOG.warn("Unable to launch worker %s. Required cpu: %f, Required mem: %f. Available aggregatedOffers : %s",
                   workerHost, requiredCpu, requiredMem, aggregatedOffers);
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

    FrameworkInfo.Builder finfo = FrameworkInfo.newBuilder()
        .setName(frameworkName)
        .setFailoverTimeout(failoverTimeout.doubleValue())
        .setUser("")
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
