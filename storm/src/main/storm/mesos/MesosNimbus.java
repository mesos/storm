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
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
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
  public static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  private static final Logger LOG = LoggerFactory.getLogger(MesosNimbus.class);
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
    taskIDtoOfferMap = Collections.synchronizedMap(new LinkedHashMap<Protos.TaskID, Protos.Offer>(intLruCacheSize + 1, .75F, true) {
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
      LOG.debug("resourceOffers: Currently have {} offers buffered {}",
                _offers.size(), (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));

      for (Protos.Offer offer : offers) {
        if (isHostAccepted(offer.getHostname())) {
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

  public void taskLost(final TaskID taskId) {
    timerScheduler.schedule(new Runnable() {
      @Override
      public void run() {
        taskIDtoOfferMap.remove(taskId);
      }
    }, MesosCommon.getSuicideTimeout(_conf), TimeUnit.SECONDS);
  }

  protected void createLocalServerPort() {
    Integer port = (Integer) _conf.get(CONF_MESOS_LOCAL_FILE_SERVER_PORT);
    LOG.debug("LocalFileServer configured to listen on port: {}", port);
    _localFileServerPort = Optional.fromNullable(port);
  }

  protected void setupHttpServer() throws Exception {
    _httpServer = new LocalFileServer();
    _configUrl = _httpServer.serveDir("/generated-conf", _generatedConfPath.toString(), _localFileServerPort);

    LOG.info("Started HTTP server from which config for the MesosSupervisor's may be fetched. URL: {}", _configUrl);
  }

  protected MesosSchedulerDriver createMesosDriver() throws IOException {
    MesosSchedulerDriver driver;
    Credential credential;
    FrameworkInfo.Builder finfo = createFrameworkBuilder();

    if ((credential = getCredential(finfo)) != null) {
      driver = new MesosSchedulerDriver(_scheduler, finfo.build(), (String) _conf.get(CONF_MASTER_URL), credential);
    } else {
      driver = new MesosSchedulerDriver(_scheduler, finfo.build(), (String) _conf.get(CONF_MASTER_URL));
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

  /**
   * Method checks if all topologies that need assignment already have supervisor running on the node where the Offer
   * comes from. Required for more accurate available resource calculation where we can exclude supervisor's demand from
   * the Offer.
   * Unfortunately because of WorkerSlot type is not topology agnostic, we need to exclude supervisor's resources only
   * in case where ALL topologies in 'allSlotsAvailableForScheduling' method satisfy condition of supervisor existence
   *
   * @param offer                        Offer
   * @param existingSupervisors          Supervisors which already placed on the node for the Offer
   * @param topologiesMissingAssignments Topology ids required assignment
   * @return Boolean value indicating supervisor existence
   */
  private boolean supervisorExists(
      Offer offer, Collection<SupervisorDetails> existingSupervisors, Set<String> topologiesMissingAssignments) {
    boolean alreadyExists = true;
    for (String topologyId : topologiesMissingAssignments) {
      String offerHost = offer.getHostname();
      boolean exists = false;
      for (SupervisorDetails d : existingSupervisors) {
        if (d.getId().equals(MesosCommon.supervisorId(offerHost, topologyId))) {
          exists = true;
        }
      }
      alreadyExists = (alreadyExists && exists);
    }
    return alreadyExists;
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

  private OfferID findOffer(WorkerSlot worker) {
    int port = worker.getPort();
    for (Offer offer : _offers.values()) {
      if (offer.getHostname().equals(worker.getNodeId())) {
        List<Resource> r = getResourcesRange(offer.getResourcesList(), port, "ports");
        if (r != null) return offer.getId();
      }
    }
    // Still haven't found the slot? Maybe it's an offer we already used.
    return null;
  }

  protected List<Resource> getResourcesScalar(final List<Resource> offerResources,
                                              final double value,
                                              final String name) {
    List<Resource> resources = new ArrayList<>();
    double valueNeeded = value;
    for (Resource r : offerResources) {
      if (r.hasReservation()) {
        // skip resources with dynamic reservations
        continue;
      }
      if (r.getType() == Type.SCALAR &&
          r.getName().equals(name)) {
        if (r.getScalar().getValue() > valueNeeded) {
          resources.add(
              r.toBuilder()
                  .setScalar(Scalar.newBuilder().setValue(valueNeeded))
                  .build()
          );
          return resources;
        } else if (Math.abs(r.getScalar().getValue() - valueNeeded) < 0.0001) { // check if zero
          resources.add(
              r.toBuilder()
                  .setScalar(Scalar.newBuilder().setValue(valueNeeded))
                  .build()
          );
          return resources;
        } else {
          resources.add(r.toBuilder().build());
          valueNeeded -= r.getScalar().getValue();
        }
      }
    }
    return resources;
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

  protected List<Resource> getResourcesRange(final List<Resource> offerResources,
                                             final long value,
                                             final String name) {
    for (Resource r : offerResources) {
      if (r.hasReservation()) {
        // skip reserved resources
        continue;
      }
      if (r.getType() == Type.RANGES && r.getName().equals(name)) {
        for (Range range : r.getRanges().getRangeList()) {
          if (value >= range.getBegin() && value <= range.getEnd()) {
            return Arrays.asList(r.toBuilder()
                .setRanges(
                    Ranges.newBuilder()
                        .addRange(
                            Range.newBuilder().setBegin(value).setEnd(value).build()
                        ).build()
                )
                .build()
            );
          }
        }
      }
    }
    return new ArrayList<>();
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
                  topologyId, slot, MesosCommon.topologyWorkerCpu(_conf, details), MesosCommon.topologyWorkerMem(_conf, details));
      }
    }

    synchronized (_offersLock) {
      computeLaunchList(topologies, slotsForTopologiesNeedingAssignments);
    }
  }

  /**
   *  @param topologies                             - Information about all submitted topologies
   *  @param slotsForTopologiesNeedingAssignments   - A map of topology name and collection of worker slots that are assigned to the topology
   *                                                  that need assignments
   */
  protected void computeLaunchList(Topologies topologies, Map<String, Collection<WorkerSlot>> slotsForTopologiesNeedingAssignments) {
    Map<OfferID, List<LaunchTask>> toLaunch = new HashMap<>();
    // For every topology that needs assignment
    for (String topologyId : slotsForTopologiesNeedingAssignments.keySet()) {
      Map<OfferID, List<WorkerSlot>> offerIDtoWorkerSlotMap = new HashMap<>();
      // Find a corresponding offer for every slot that needs to be launched
      for (WorkerSlot slot : slotsForTopologiesNeedingAssignments.get(topologyId)) {
        OfferID id = findOffer(slot);
        if (!offerIDtoWorkerSlotMap.containsKey(id)) {
          offerIDtoWorkerSlotMap.put(id, new ArrayList<WorkerSlot>());
        }
        /*
         * Find offer could return "null" or an OfferID
         * If a port that is specified in the workerSlot is found, findOffer returns the OfferID associated with the port
         * If a port is not found, findOffer returns null
         */
        offerIDtoWorkerSlotMap.get(id).add(slot);
      }

      // At this point, we have a map of OfferID and WorkerSlot in the form of offerIDtoWorkerSlotMap
      // Note that at this point, we only know that the ports are available on the node. We still have to
      // find cpu and memory in order to launch the workers
      for (OfferID id : offerIDtoWorkerSlotMap.keySet()) {
        computeResourcesForSlot(_offers, topologies, toLaunch, topologyId, offerIDtoWorkerSlotMap, id);
      }
    }

    for (OfferID id : toLaunch.keySet()) {
      List<LaunchTask> tasks = toLaunch.get(id);
      List<TaskInfo> launchList = new ArrayList<>();

      LOG.info("Launching tasks for offerId: {} : {}", id.getValue(), launchTaskListToString(tasks));
      for (LaunchTask t : tasks) {
        launchList.add(t.getTask());
        taskIDtoOfferMap.put(t.getTask().getTaskId(), t.getOffer());
      }

      List<OfferID> launchOffer = new ArrayList<>();
      launchOffer.add(id);
      _driver.launchTasks(launchOffer, launchList);
      _offers.remove(id);
    }
  }

  /**
   *  Considering the way this method is invoked - computeResourcesForSlot(_offers, topologies, toLaunch, topologyId, offerIDtoWorkerSlotMap, id) -
   *  the following params can be removed/refactored.
   *    1) offers - Method could just use the _offers private variable directly.
   *    2) toLaunch - This should be a return value
   *    3) offerId/offerIDtoWorkerSlotMap - Passing both of these params is redundant
   * @param offers                     -  All available offers.
   * @param topologies                 -  Information about all submitted topologies
   * @param toLaunch                   -  A map of offerID and list of tasks that needs to be launched using the OfferID
   * @param topologyId                 -  TopologyId for which we are computing resources
   * @param offerIDtoWorkerSlotMap     -  Map of OfferID that contains ports for the list of workerSlots
   * @param offerId                    -  One of the keys in offerIDtoWorkerSlotMap
   */
  protected void computeResourcesForSlot(final RotatingMap<OfferID, Offer> offers,
                                         Topologies topologies,
                                         Map<OfferID, List<LaunchTask>> toLaunch,
                                         String topologyId,
                                         Map<OfferID, List<WorkerSlot>> offerIDtoWorkerSlotMap,
                                         OfferID offerId) {
    boolean usingExistingOffer = false;
    boolean subtractedExecutorResources = false;
    Offer offer = offers.get(offerId);
    List<WorkerSlot> workerSlots = offerIDtoWorkerSlotMap.get(offerId);

    // Note: As of writing this comment, the workerSlots belong to same topology
    for (WorkerSlot slot : workerSlots) {
      TopologyDetails details = topologies.getById(topologyId);
      String workerPrefix = "";
      if (_conf.containsKey(MesosCommon.WORKER_NAME_PREFIX)) {
        workerPrefix = MesosCommon.getWorkerPrefix(_conf, details);
      }
      TaskID taskId = TaskID.newBuilder()
          .setValue(MesosCommon.taskId(workerPrefix + slot.getNodeId(), slot.getPort()))
          .build();

      // taskIDtoOfferMap is unnecessary
      //    1. OfferID is usable only once - That is if we ask mesos to launch task(s) on an offerID,
      //       mesos returns remnants as a different offer with a completely different offerID
      //    2. If MesosNimbus is restarted, all this information is lost.
      //    3. By not clearing taskIDtoOfferMap at the end of this method, we are only wasting memory
      // The following if condition to check taskIDtoOfferMap is unnecessary
      //    1. This function is invoked only once per slot.
      //    2. If the function is invoked a second time for the same slot, then it's either because the task
      //       was finished or killed. Either way, the old offerId we used to launch the worker is invalid
      if ((offerId == null || offer == null) && taskIDtoOfferMap.containsKey(taskId)) {
        offer = taskIDtoOfferMap.get(taskId);
        if (offer != null) {
          offerId = offer.getId();
          usingExistingOffer = true;
        }
      }

      // The following way of finding resources for the slot is bad!
      // Suppose we have the following offers:
      //     o1 - { host: h1 ports : 31000-32000 }
      //     o2 - { host: h1 mem : 30000 cpu : 24 }
      // because the offers are fragmented, they are useless
      //   1. At this point a worker slot "h1-3000" associated with o1 for instance wouldn't
      //      be launched because it doesn't have mem and cpu
      //   2. o2 is useless because it doesn't have any ports and therefore won't be used at all
      if (offerId != null && offer != null) {
        // The fact that we are here means that this offer has a port. We need to find if it also has
        // enough memory and cpu to launch the worker.
        if (!toLaunch.containsKey(offerId)) {
          toLaunch.put(offerId, new ArrayList<LaunchTask>());
        }
        double workerCpu = MesosCommon.topologyWorkerCpu(_conf, details);
        double workerMem = MesosCommon.topologyWorkerMem(_conf, details);
        double executorCpu = MesosCommon.executorCpu(_conf);
        double executorMem = MesosCommon.executorMem(_conf);

        Map executorData = new HashMap();
        executorData.put(MesosCommon.SUPERVISOR_ID, MesosCommon.supervisorId(slot.getNodeId(), details.getId()));
        executorData.put(MesosCommon.ASSIGNMENT_ID, workerPrefix + slot.getNodeId());

        Offer.Builder newBuilder = Offer.newBuilder();
        newBuilder.mergeFrom(offer);
        newBuilder.clearResources();

        Offer.Builder existingBuilder = Offer.newBuilder();
        existingBuilder.mergeFrom(offer);
        existingBuilder.clearResources();
        String extraConfig = "";

        List<Resource> offerResources = new ArrayList<>();
        offerResources.addAll(offer.getResourcesList());
        // Prefer reserved resources?
        if (_preferReservedResources) {
          Collections.sort(offerResources, new ResourceRoleComparator());
        }

        List<Resource> executorCpuResources = getResourcesScalar(offerResources, executorCpu, "cpus");
        List<Resource> executorMemResources = getResourcesScalar(offerResources, executorMem, "mem");
        List<Resource> executorPortsResources = null;

        // Question(ksoundararaj):
        // Shouldn't we be validating executorCpuResources and executorCpuResources to ensure they aren't
        // empty or less than what was requested?
        if (!subtractedExecutorResources) {
          offerResources = subtractResourcesScalar(offerResources, executorCpu, "cpus");
          offerResources = subtractResourcesScalar(offerResources, executorMem, "mem");
          subtractedExecutorResources = true;
        }
        List<Resource> workerCpuResources = getResourcesScalar(offerResources, workerCpu, "cpus");
        offerResources = subtractResourcesScalar(offerResources, workerCpu, "cpus");
        List<Resource> workerMemResources = getResourcesScalar(offerResources, workerMem, "mem");
        offerResources = subtractResourcesScalar(offerResources, workerMem, "mem");
        List<Resource> workerPortsResources = getResourcesRange(offerResources, slot.getPort(), "ports");
        offerResources = subtractResourcesRange(offerResources, slot.getPort(), "ports");

        // Find port for the logviewer
        if (!subtractedExecutorResources && MesosCommon.startLogViewer(_conf)) {
          List<Integer> portList = new ArrayList<>();
          collectPorts(offerResources, portList, 1);
          int port = Optional.fromNullable((Number) _conf.get(Config.LOGVIEWER_PORT)).or(8000).intValue();
          executorPortsResources = getResourcesRange(offerResources, port, "ports");
          if (!executorPortsResources.isEmpty()) {
            // Was the port available?
            extraConfig = String.format(" -c %s=true", MesosCommon.AUTO_START_LOGVIEWER_CONF);
            offerResources = subtractResourcesRange(offerResources, port, "ports");
          }
        }

        Offer remainingOffer = existingBuilder.addAllResources(offerResources).build();

        // Update the remaining offer list
        offers.put(offerId, remainingOffer);

        // At this point, hopefully, we have all the resources we need.
        String configUri;
        try {
          configUri = new URL(_configUrl.toURL(), _configUrl.getPath() + "/storm.yaml").toString();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }

        String delimiter = MesosCommon.getMesosComponentNameDelimiter(_conf, details);
        String topologyAndNodeId = String.format("%s%s%s", details.getId(), delimiter, slot.getNodeId());
        String executorName = String.format("storm-supervisor%s%s", delimiter, topologyAndNodeId);
        String taskName = String.format("storm-worker%s%s:%d", delimiter, topologyAndNodeId, slot.getPort());
        String executorDataStr = JSONValue.toJSONString(executorData);
        ExecutorInfo.Builder executorInfoBuilder = ExecutorInfo.newBuilder();
        executorInfoBuilder
            .setName(executorName)
            .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
            .setData(ByteString.copyFromUtf8(executorDataStr))
            .addAllResources(executorCpuResources)
            .addAllResources(executorMemResources);
        if (executorPortsResources != null) {
          executorInfoBuilder.addAllResources(executorPortsResources);
        }
        ICommandLineShim commandLineShim = CommandLineShimFactory.makeCommandLineShim(_container.isPresent(), extraConfig);
        if (_container.isPresent()) {
          executorInfoBuilder
              .setCommand(CommandInfo.newBuilder()
                  .addUris(URI.newBuilder().setValue(configUri))
                  .setValue(commandLineShim.getCommandLine(details.getId())))
              .setContainer(
                  ContainerInfo.newBuilder()
                      .setType(ContainerInfo.Type.DOCKER)
                      .setDocker(
                          ContainerInfo.DockerInfo.newBuilder()
                              .setImage(_container.get())
                              .setNetwork(ContainerInfo.DockerInfo.Network.HOST)
                              .setForcePullImage(true)
                              .build()
                      )
                      .build()
            );
        } else {
          executorInfoBuilder
              .setCommand(CommandInfo.newBuilder()
                  .addUris(URI.newBuilder().setValue((String) _conf.get(CONF_EXECUTOR_URI)))
                  .addUris(URI.newBuilder().setValue(configUri))
                  .setValue(commandLineShim.getCommandLine(details.getId())));
        }

        LOG.info("Launching task with Mesos Executor data: < {} >", executorDataStr);
        TaskInfo task = TaskInfo.newBuilder()
            .setName(taskName)
            .setTaskId(taskId)
            .setSlaveId(offer.getSlaveId())
            .setExecutor(executorInfoBuilder.build())
            .addAllResources(workerCpuResources)
            .addAllResources(workerMemResources)
            .addAllResources(workerPortsResources)
            .build();

        Offer newOffer = offer.toBuilder()
            .addAllResources(task.getResourcesList()).build();

        LOG.debug("Launching task: {}", task.toString());

        toLaunch.get(offerId).add(new LaunchTask(task, newOffer));
      }

      if (usingExistingOffer) {
        _driver.killTask(taskId);
      }
    }
  }

  private FrameworkInfo.Builder createFrameworkBuilder() throws IOException {
    Number failoverTimeout = Optional.fromNullable((Number) _conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS)).or(24 * 7 * 3600);
    String role = Optional.fromNullable((String) _conf.get(CONF_MESOS_ROLE)).or("*");
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
