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
import org.apache.log4j.Logger;
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
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;
import org.yaml.snakeyaml.Yaml;
import storm.mesos.schedulers.DefaultScheduler;
import storm.mesos.schedulers.IMesosStormScheduler;
import storm.mesos.schedulers.OfferResources;
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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
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

import static storm.mesos.util.PrettyProtobuf.offerMapToString;
import static storm.mesos.util.PrettyProtobuf.taskInfoListToString;
import static storm.mesos.util.PrettyProtobuf.offerIDListToString;

public class MesosNimbus implements INimbus {
  public static final String CONF_EXECUTOR_URI = "mesos.executor.uri";
  public static final String CONF_MASTER_URL = "mesos.master.url";
  public static final String CONF_MASTER_FAILOVER_TIMEOUT_SECS = "mesos.master.failover.timeout.secs";
  public static final String CONF_MESOS_ALLOWED_HOSTS = "mesos.allowed.hosts";
  public static final String CONF_MESOS_DISALLOWED_HOSTS = "mesos.disallowed.hosts";
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
  public static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  private static final Logger LOG = Logger.getLogger(MesosNimbus.class);
  private final Object _offersLock = new Object();
  protected java.net.URI _configUrl;
  LocalStateShim _state;
  NimbusScheduler _scheduler;
  volatile SchedulerDriver _driver;
  Timer _timer = new Timer();
  Map _conf;
  Set<String> _allowedHosts;
  Set<String> _disallowedHosts;
  Optional<Integer> _localFileServerPort;
  private RotatingMap<OfferID, Offer> _offers;
  private LocalFileServer _httpServer;
  private Map<TaskID, Offer> _usedOffers;
  private ScheduledExecutorService timerScheduler = Executors.newScheduledThreadPool(1);
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
    return null;
  }

  @Override
  public String getHostName(Map<String, SupervisorDetails> map, String nodeId) {
    return nodeId;
  }

  @Override
  public void prepare(Map conf, String localDir) {
    try {
      initialize(conf, localDir);

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
  protected void initialize(Map conf, String localDir) throws Exception {
    _conf = new HashMap();
    _conf.putAll(conf);

    _state = new LocalStateShim(localDir);
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

    createLocalServerPort();
    setupHttpServer();

    _conf.put(Config.NIMBUS_HOST, _configUrl.getHost());

    File generatedConf = Paths.get(_generatedConfPath.toString(), "storm.yaml").toFile();
    Yaml yaml = new Yaml();
    FileWriter writer = new FileWriter(generatedConf);
    yaml.dump(_conf, writer);
  }


  public void doRegistration(final SchedulerDriver driver, Protos.FrameworkID id) {
    _driver = driver;
    _state.put(FRAMEWORK_ID, id.getValue());
    Number filterSeconds = Optional.fromNullable((Number) _conf.get(CONF_MESOS_OFFER_FILTER_SECONDS)).or(120);
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

    Number lruCacheSize = Optional.fromNullable((Number) _conf.get(CONF_MESOS_OFFER_LRU_CACHE_SIZE)).or(1000);
    final int intLruCacheSize = lruCacheSize.intValue();
    _usedOffers = Collections.synchronizedMap(new LinkedHashMap<Protos.TaskID, Protos.Offer>(intLruCacheSize + 1, .75F, true) {
      // This method is called just after a new entry has been added
      public boolean removeEldestEntry(Map.Entry eldest) {
        return size() > intLruCacheSize;
      }
    });

    Number offerExpired = Optional.fromNullable((Number) _conf.get(Config.NIMBUS_MONITOR_FREQ_SECS)).or(10);
    Number expiryMultiplier = Optional.fromNullable((Number) _conf.get(CONF_MESOS_OFFER_EXPIRY_MULTIPLIER)).or(2.5);
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
      LOG.debug("resourceOffers: Currently have " + _offers.size() + " offers buffered" +
          (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));
      for (Protos.Offer offer : offers) {
        if (isHostAccepted(offer.getHostname())) {
          LOG.debug("resourceOffers: Recording offer from host: " +
              offer.getHostname() + ", offerId: " + offer.getId().getValue());
          _offers.put(offer.getId(), offer);
        } else {
          LOG.debug("resourceOffers: Declining offer from host: " +
              offer.getHostname() + ", offerId: " + offer.getId().getValue());
          driver.declineOffer(offer.getId());
        }
      }
      LOG.debug("resourceOffers: After processing offers, now have " +
                _offers.size() + " offers buffered:" + offerMapToString(_offers));
    }
  }

  public void offerRescinded(OfferID id) {
    synchronized (_offersLock) {
      _offers.remove(id);
    }
  }

  public void taskLost(final TaskID taskId) {
    timerScheduler.schedule(new Runnable() {
      @Override
      public void run() {
        _usedOffers.remove(taskId);
      }
    }, MesosCommon.getSuicideTimeout(_conf), TimeUnit.SECONDS);
  }

  protected void createLocalServerPort() {
    Integer port = (Integer) _conf.get(CONF_MESOS_LOCAL_FILE_SERVER_PORT);
    LOG.debug("LocalFileServer configured to listen on port: " + port);
    _localFileServerPort = Optional.fromNullable(port);
  }

  protected void setupHttpServer() throws Exception {
    _httpServer = new LocalFileServer();
    _configUrl = _httpServer.serveDir("/generated-conf", _generatedConfPath.toString(), _localFileServerPort);

    LOG.info("Started HTTP server from which config for the MesosSupervisor's may be fetched. URL: " + _configUrl);
  }

  protected MesosSchedulerDriver createMesosDriver() throws IOException {
    MesosSchedulerDriver driver;
    Credential credential;
    FrameworkInfo.Builder finfo = createFrameworkBuilder();

    if ((credential = getCredential(finfo)) != null) {
      driver = new MesosSchedulerDriver(_scheduler,
          finfo.build(),
          (String) _conf.get(CONF_MASTER_URL),
          credential);
    } else {
      driver = new MesosSchedulerDriver(_scheduler,
          finfo.build(),
          (String) _conf.get(CONF_MASTER_URL));
    }

    return driver;
  }

  private void collectPorts(List<Resource> offers, List<Integer> portList, int maxPorts) {
    for (Resource r : offers) {
      if (r.getName().equals("ports")) {
        for (Range range : r.getRanges().getRangeList()) {
          if (portList.size() >= maxPorts) {
            break;
          } else {
            int start = (int) range.getBegin();
            int end = (int) range.getEnd();
            for (int p = start; p <= end; p++) {
              portList.add(p);
              if (portList.size() >= maxPorts) {
                break;
              }
            }
          }
        }
      }
    }
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

  protected List<Resource> subtractResourcesScalar(final List<Resource> offerResources,
                                                   final double value,
                                                   final String name) {
    List<Resource> resources = new ArrayList<>();
    double valueNeeded = value;
    for (Resource r : offerResources) {
      if (r.hasReservation()) {
        // skip reserved resources
        continue;
      }
      if (r.getType() == Type.SCALAR &&
          r.getName().equals(name)) {
        if (r.getScalar().getValue() > valueNeeded) {
          resources.add(
              r.toBuilder()
                  .setScalar(Scalar.newBuilder().setValue(r.getScalar().getValue() - valueNeeded))
                  .build()
          );
          valueNeeded = 0;
        } else {
          valueNeeded -= r.getScalar().getValue();
        }
      } else {
        resources.add(r.toBuilder().build());
      }
    }
    return resources;
  }


  protected List<Resource> subtractResourcesRange(final List<Resource> offerResources,
                                                  final long value,
                                                  final String name) {
    List<Resource> resources = new ArrayList<>();
    for (Resource r : offerResources) {
      if (r.hasReservation()) {
        // skip reserved resources
        continue;
      }
      if (r.getType() == Type.RANGES && r.getName().equals(name)) {
        Resource.Builder remaining = r.toBuilder();
        Ranges.Builder ranges = Ranges.newBuilder();
        for (Range range : r.getRanges().getRangeList()) {
          if (value >= range.getBegin() && value <= range.getEnd()) {
            if (range.getBegin() <= value && range.getEnd() >= value) {
              if (range.getBegin() == value && range.getEnd() > value) {
                ranges.addRange(
                    Range.newBuilder().setBegin(range.getBegin() + 1).setEnd(range.getEnd()).build()
                );
              } else if (range.getBegin() < value && range.getEnd() == value) {
                ranges.addRange(
                    Range.newBuilder().setBegin(range.getBegin()).setEnd(range.getEnd() - 1).build()
                );
              } else if (range.getBegin() < value && range.getEnd() > value) {
                ranges.addRange(
                    Range.newBuilder().setBegin(range.getBegin()).setEnd(value - 1).build()
                );
                ranges.addRange(
                    Range.newBuilder().setBegin(value + 1).setEnd(range.getEnd()).build()
                );
              } else {
                // skip
              }
            }
          } else {
            ranges.addRange(range.toBuilder().build());
          }
        }
        remaining.setRanges(ranges.build());
        if (remaining.getRanges().getRangeCount() > 0) {
          resources.add(remaining.build());
        }
      } else {
        resources.add(r.toBuilder().build());
      }
    }
    return resources;
  }

  private List<Integer> getPortList(List<Resource> offers, int maxPorts) {
    List<Integer> portList = new ArrayList<>();
    for (Resource r : offers) {
      if (r.getName().equals("ports")) {
        for (Range range : r.getRanges().getRangeList()) {
          if (portList.size() >= maxPorts) {
            break;
          } else {
            int start = (int) range.getBegin();
            int end = (int) range.getEnd();
            for (int p = start; p <= end; p++) {
              portList.add(p);
              if (portList.size() >= maxPorts) {
                break;
              }
            }
          }
        }
      }
    }
    return portList;
  }



  private String getLogViewerConfig() {
    String logViewerConfig = null;
    // Find port for the logviewer
    return " -c " + MesosCommon.AUTO_START_LOGVIEWER_CONF + "=true";
  }

  private ExecutorInfo.Builder getExecutorInfoBuilder(TopologyDetails details, String executorDataStr,
                                                      String executorName,
                                                      List<Resource> executorCpuResources, List<Resource> executorMemResources, List<Resource> executorPortsResources, String extraConfig) {
    String configUri;
    try {
      configUri = new URL(_configUrl.toURL(),
                          _configUrl.getPath() + "/storm.yaml").toString();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    ExecutorInfo.Builder executorInfoBuilder = ExecutorInfo.newBuilder();

    executorInfoBuilder
      .setName(executorName)
      .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
      .setData(ByteString.copyFromUtf8(executorDataStr))
      .addAllResources(executorCpuResources)
      .addAllResources(executorMemResources)
      .addAllResources(executorPortsResources);

    ICommandLineShim commandLineShim = CommandLineShimFactory.makeCommandLineShim(_container.isPresent(), extraConfig);
    if (_container.isPresent()) {
      executorInfoBuilder.setCommand(CommandInfo.newBuilder()
                                                .addUris(URI.newBuilder().setValue(configUri))
                                                .setValue(commandLineShim.getCommandLine()))
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
                                                .addUris(URI.newBuilder().setValue((String) _conf.get(CONF_EXECUTOR_URI)))
                                                .addUris(URI.newBuilder().setValue(configUri))
                                                .setValue(commandLineShim.getCommandLine()));
    }

    return executorInfoBuilder;
  }



  public Map<String, List<TaskInfo>> getTasksToLaunch(Topologies topologies,
                                                     Map<String, Collection<WorkerSlot>> slots,
                                                     Map<String, OfferResources> offerResourcesPerNode) {
    Map<String, List<TaskInfo>> tasksToLaunchPerNode = new HashMap<>();

    for (String topologyId : slots.keySet()) {
      Collection<WorkerSlot> slotList = slots.get(topologyId);
      TopologyDetails details = topologies.getById(topologyId);
      boolean subtractedExecutorResources = false;

      double workerCpu = MesosCommon.topologyWorkerCpu(_conf, details);
      double workerMem = MesosCommon.topologyWorkerMem(_conf, details);
      double executorCpu = MesosCommon.executorCpu(_conf);
      double executorMem = MesosCommon.executorMem(_conf);

      for (WorkerSlot slot : slotList) {
        OfferResources offerResources = offerResourcesPerNode.get(slot.getNodeId());
        String workerPrefix = "";
        if (_conf.containsKey(MesosCommon.WORKER_NAME_PREFIX)) {
          workerPrefix = MesosCommon.getWorkerPrefix(_conf, details);
        }

        List<OfferID> ids = offerResources.getOfferIds();

        if (ids.isEmpty()) {
          LOG.warn("Unable to find offer for slot: " + slot + " as it is no longer in the RotatingMap of offers, " +
                   " topology " + topologyId + " will no longer be scheduled on this slot");
        }


        TaskID taskId = TaskID.newBuilder()
                              .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort()))
                              .build();

        if (!subtractedExecutorResources) {
          workerCpu += executorCpu;
          workerMem += executorMem;
        }

        Map executorData = new HashMap();
        executorData.put(MesosCommon.SUPERVISOR_ID, MesosCommon.supervisorId(slot.getNodeId(), details.getId()));
        executorData.put(MesosCommon.ASSIGNMENT_ID, workerPrefix + slot.getNodeId());

        if (!subtractedExecutorResources) {
          workerCpu -= executorCpu;
          workerMem -= executorMem;
          subtractedExecutorResources = true;
        }

        String topologyAndNodeId = details.getId() + " | " + slot.getNodeId();
        String executorName = "storm-supervisor | " + topologyAndNodeId;
        String taskName = "storm-worker | " + topologyAndNodeId + ":" + slot.getPort();
        String executorDataStr = JSONValue.toJSONString(executorData);

        // The fact that we are here implies that the resources are available for the worker in the host.
        // So we dont have to check if the resources are actually available before proceding.
        if (executorCpu > offerResources.getCpu() || executorMem > offerResources.getMem()) {
          LOG.error(String.format("Unable to launch worker %s. Required executorCpu: %d, Required executorMem: %d. Available OfferResources : %s", offerResources.getHostName(), executorCpu, executorMem, offerResources));
          continue;
        }
        List<Resource> executorCpuResources = offerResources.getResourcesListScalar(executorCpu, "cpu", MesosCommon.getRole(_conf));
        List<Resource> executorMemResources = offerResources.getResourcesListScalar(executorMem, "mem", MesosCommon.getRole(_conf));
        List<Resource> executorPortResources = new ArrayList<>();

        String extraConfig = "";
        if (MesosCommon.autoStartLogViewer(_conf)) {
          long port = Optional.fromNullable((Number) _conf.get(Config.LOGVIEWER_PORT)).or(8000).intValue();;
          List<Resource> logviewerPortResources = offerResources.getResourcesRange(port, "ports");
          if (logviewerPortResources != null) {
            extraConfig = getLogViewerConfig();
            executorPortResources.addAll(logviewerPortResources);
          }
          LOG.error(String.format("Unable to launch logviewer on worker %s:%d. Port could not be found. Available OfferResources : %s", offerResources.getHostName(), port, offerResources));
        }

        ExecutorInfo.Builder executorInfoBuilder = getExecutorInfoBuilder(details, executorDataStr, executorName, executorCpuResources, executorMemResources, executorPortResources, extraConfig);

        TaskInfo task = TaskInfo.newBuilder()
                                .setTaskId(taskId)
                                .setName(taskName)
                                .setSlaveId(offerResources.getSlaveId())
                                .setExecutor(executorInfoBuilder.build())
                                .addAllResources(offerResources.getResourcesListScalar(workerCpu, "cpus", MesosCommon.getRole(_conf)))
                                .addAllResources(offerResources.getResourcesListScalar(workerMem, "mem", MesosCommon.getRole(_conf)))
                                .addAllResources(offerResources.getResourcesRange("ports"))
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

  @Override
  public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slots) {
    synchronized (_offersLock) {
      if (slots.size() == 0) {
        LOG.info("assignSlots: no slots passed in, nothing to do");
        return;
      }

      Map<String, OfferResources> offerResourcesPerNode = MesosCommon.getConsolidatedOfferResourcesPerNode(_conf, _offers);
      Map<String, List<TaskInfo>> tasksToLaunchPerNode = getTasksToLaunch(topologies, slots, offerResourcesPerNode);

      for (String node : tasksToLaunchPerNode.keySet()) {
        List<OfferID> offerIDList = offerResourcesPerNode.get(node).getOfferIds();
        List<TaskInfo> taskInfoList = tasksToLaunchPerNode.get(node);

        LOG.info("Using offerIDs: " + offerIDListToString(offerIDList) + " on host: " + node + " to launch tasks: " + taskInfoListToString(taskInfoList));

        _driver.launchTasks(offerIDList, taskInfoList);
        for (OfferID offerID: offerIDList) {
          _offers.remove(offerID);
        }
      }
    }
  }

  private FrameworkInfo.Builder createFrameworkBuilder() throws IOException {
    Number failoverTimeout = Optional.fromNullable((Number) _conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS)).or(24 * 7 * 3600);
    String role = MesosCommon.getRole(_conf);
    Boolean checkpoint = Optional.fromNullable((Boolean) _conf.get(CONF_MESOS_CHECKPOINT)).or(false);
    String frameworkName = Optional.fromNullable((String) _conf.get(CONF_MESOS_FRAMEWORK_NAME)).or("Storm!!!");

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

    String principal = Optional.fromNullable((String) _conf.get(CONF_MESOS_PRINCIPAL)).orNull();
    String secretFilename = Optional.fromNullable((String) _conf.get(CONF_MESOS_SECRET_FILE)).orNull();

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
