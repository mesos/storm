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
import backtype.storm.scheduler.*;
import backtype.storm.utils.LocalState;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static storm.mesos.PrettyProtobuf.*;

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
  private static final Logger LOG = Logger.getLogger(MesosNimbus.class);
  private final Object _offersLock = new Object();
  protected java.net.URI _configUrl;
  LocalState _state;
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
  private ScheduledExecutorService timerScheduler =
      Executors.newScheduledThreadPool(1);
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

    _state = new LocalState(localDir);
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
    try {
      _state.put(FRAMEWORK_ID, id.getValue());
    } catch (IOException e) {
      LOG.error("Halting process...", e);
      Runtime.getRuntime().halt(1);
    }
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

  protected OfferResources getResources(Offer offer, double executorCpu, double executorMem, double cpu, double mem) {
    OfferResources resources = new OfferResources();

    double offerCpu = 0;
    double offerMem = 0;

    for (Resource r : offer.getResourcesList()) {
      if (r.hasReservation()) {
        // skip resources with reservations
        continue;
      }
      if (r.getType() == Type.SCALAR) {
        if (r.getName().equals("cpus")) {
          offerCpu += r.getScalar().getValue();
        } else if (r.getName().equals("mem")) {
          offerMem += r.getScalar().getValue();
        }
      }
    }

    if (offerCpu >= executorCpu + cpu &&
        offerMem >= executorMem + mem) {
      resources.cpuSlots = (int) Math.floor((offerCpu - executorCpu) / cpu);
      resources.memSlots = (int) Math.floor((offerMem - executorMem) / mem);
    }

    int maxPorts = Math.min(resources.cpuSlots, resources.memSlots);

    List<Integer> portList = new ArrayList<>();
    collectPorts(offer.getResourcesList(), portList, maxPorts);
    resources.ports.addAll(portList);

    LOG.debug("Offer: " + offerToString(offer));
    LOG.debug("Extracted resources: " + resources.toString());
    return resources;
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

  private List<WorkerSlot> toSlots(Offer offer, double cpu, double mem, boolean supervisorExists) {
    double executorCpuDemand = supervisorExists ? 0 : MesosCommon.executorCpu(_conf);
    double executorMemDemand = supervisorExists ? 0 : MesosCommon.executorMem(_conf);

    OfferResources resources = getResources(
        offer,
        executorCpuDemand,
        executorMemDemand,
        cpu,
        mem);

    List<WorkerSlot> ret = new ArrayList<WorkerSlot>();
    int availableSlots = Math.min(resources.cpuSlots, resources.memSlots);
    availableSlots = Math.min(availableSlots, resources.ports.size());
    for (int i = 0; i < availableSlots; i++) {
      ret.add(new WorkerSlot(offer.getHostname(), resources.ports.get(i)));
    }
    return ret;
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
      LOG.debug("allSlotsAvailableForScheduling: Currently have " + _offers.size() + " offers buffered" +
          (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));
      if (!topologiesMissingAssignments.isEmpty()) {
        LOG.info("Topologies that need assignments: " + topologiesMissingAssignments.toString());

        // Revive any filtered offers
        _driver.reviveOffers();
      } else {
        LOG.info("Declining offers because no topologies need assignments");
        _offers.clear();
        return new ArrayList<>();
      }
    }

    Double cpu = null;
    Double mem = null;
    // TODO: maybe this isn't the best approach. if a topology raises #cpus keeps failing,
    // it will mess up scheduling on this cluster permanently
    for (String id : topologiesMissingAssignments) {
      TopologyDetails details = topologies.getById(id);
      double tcpu = MesosCommon.topologyWorkerCpu(_conf, details);
      double tmem = MesosCommon.topologyWorkerMem(_conf, details);
      if (cpu == null || tcpu > cpu) {
        cpu = tcpu;
      }
      if (mem == null || tmem > mem) {
        mem = tmem;
      }
    }

    LOG.info("allSlotsAvailableForScheduling: pending topologies' max resource requirements per worker: cpu: " +
        String.valueOf(cpu) + " & mem: " + String.valueOf(mem));

    List<WorkerSlot> allSlots = new ArrayList<>();

    if (cpu != null && mem != null) {
      synchronized (_offersLock) {
        for (Offer offer : _offers.newestValues()) {
          boolean supervisorExists = supervisorExists(offer, existingSupervisors, topologiesMissingAssignments);
          List<WorkerSlot> offerSlots = toSlots(offer, cpu, mem, supervisorExists);
          if (offerSlots.isEmpty()) {
            _offers.clearKey(offer.getId());
            LOG.debug("Declining offer `" + offerToString(offer) + "' because it wasn't " +
                "usable to create a slot which fits largest pending topologies' aggregate needs " +
                "(max cpu: " + String.valueOf(cpu) + " max mem: " + String.valueOf(mem) + ")");
          } else {
            allSlots.addAll(offerSlots);
          }
        }
      }
    }

    LOG.info("Number of available slots: " + allSlots.size());
    if (LOG.isDebugEnabled()) {
      for (WorkerSlot slot : allSlots) {
        LOG.debug("available slot: " + slot);
      }
    }
    return allSlots;
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
        // skip resources with reservations
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

  @Override
  public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slots) {
    if (slots.size() == 0) {
      LOG.debug("assignSlots: no slots passed in, nothing to do");
      return;
    }
    for (Map.Entry<String, Collection<WorkerSlot>> topologyToSlots : slots.entrySet()) {
      String topologyId = topologyToSlots.getKey();
      for (WorkerSlot slot : topologyToSlots.getValue()) {
        TopologyDetails details = topologies.getById(topologyId);
        LOG.debug("assignSlots: topologyId: " + topologyId + " worker being assigned to slot: " + slot +
            " with workerCpu: " + MesosCommon.topologyWorkerCpu(_conf, details) +
            " workerMem: " + MesosCommon.topologyWorkerMem(_conf, details));
      }
    }
    synchronized (_offersLock) {
      computeLaunchList(topologies, slots);
    }
  }

  protected void computeLaunchList(Topologies topologies, Map<String, Collection<WorkerSlot>> slots) {
    Map<OfferID, List<LaunchTask>> toLaunch = new HashMap<>();
    for (String topologyId : slots.keySet()) {
      Map<OfferID, List<WorkerSlot>> slotList = new HashMap<>();
      for (WorkerSlot slot : slots.get(topologyId)) {
        OfferID id = findOffer(slot);
        if (!slotList.containsKey(id)) {
          slotList.put(id, new ArrayList<WorkerSlot>());
        }
        slotList.get(id).add(slot);
      }

      for (OfferID id : slotList.keySet()) {
        computeResourcesForSlot(_offers, topologies, toLaunch, topologyId, slotList, id);
      }
    }

    for (OfferID id : toLaunch.keySet()) {
      List<LaunchTask> tasks = toLaunch.get(id);
      List<TaskInfo> launchList = new ArrayList<>();

      LOG.info("Launching tasks for offerId: " + id.getValue() + ":" + launchTaskListToString(tasks));
      for (LaunchTask t : tasks) {
        launchList.add(t.getTask());
        _usedOffers.put(t.getTask().getTaskId(), t.getOffer());
      }

      List<OfferID> launchOffer = new ArrayList<>();
      launchOffer.add(id);
      _driver.launchTasks(launchOffer, launchList);
      _offers.remove(id);
    }
  }

  protected void computeResourcesForSlot(final RotatingMap<OfferID, Offer> offers,
                                         Topologies topologies,
                                         Map<OfferID, List<LaunchTask>> toLaunch,
                                         String topologyId,
                                         Map<OfferID, List<WorkerSlot>> slotList,
                                         OfferID id) {
    Offer offer = offers.get(id);
    List<WorkerSlot> workerSlots = slotList.get(id);
    boolean usingExistingOffer = false;
    boolean subtractedExecutorResources = false;

    for (WorkerSlot slot : workerSlots) {
      TopologyDetails details = topologies.getById(topologyId);
      String workerPrefix = "";
      if (_conf.containsKey(MesosCommon.WORKER_NAME_PREFIX)) {
        workerPrefix = MesosCommon.getWorkerPrefix(_conf, details);
      }
      TaskID taskId = TaskID.newBuilder()
          .setValue(MesosCommon.taskId(workerPrefix + slot.getNodeId(), slot.getPort()))
          .build();

      if ((id == null || offer == null) && _usedOffers.containsKey(taskId)) {
        offer = _usedOffers.get(taskId);
        if (offer != null) {
          id = offer.getId();
          usingExistingOffer = true;
        }
      }
      if (id != null && offer != null) {
        if (!toLaunch.containsKey(id)) {
          toLaunch.put(id, new ArrayList<LaunchTask>());
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

        List<Resource> executorCpuResources = null;
        List<Resource> executorMemResources = null;
        List<Resource> executorPortsResources = null;
        if (!subtractedExecutorResources) {
          executorCpuResources = getResourcesScalar(offerResources, executorCpu, "cpus");
          offerResources = subtractResourcesScalar(offerResources, executorCpu, "cpus");
          executorMemResources = getResourcesScalar(offerResources, executorMem, "mem");
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
            extraConfig = " -c " + MesosCommon.AUTO_START_LOGVIEWER_CONF + "=true";
            offerResources = subtractResourcesRange(offerResources, port, "ports");
          }
        }
        Offer remainingOffer = existingBuilder.addAllResources(offerResources).build();

        // Update the remaining offer list
        offers.put(id, remainingOffer);

        String configUri;
        try {
          configUri = new URL(_configUrl.toURL(),
              _configUrl.getPath() + "/storm.yaml").toString();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }

        String delimiter = MesosCommon.getMesosComponentNameDelimiter(_conf, details);
        String topologyAndNodeId = details.getId() + delimiter + slot.getNodeId();
        String executorName = "storm-supervisor" + delimiter + topologyAndNodeId;
        String taskName = "storm-worker" + delimiter + topologyAndNodeId + ":" + slot.getPort();
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
        if (_container.isPresent()) {
          // An ugly workaround for a bug in DCOS
          Map<String, String> env = System.getenv();
          String javaLibPath = env.get("MESOS_NATIVE_JAVA_LIBRARY");

          executorInfoBuilder
              .setCommand(CommandInfo.newBuilder()
                  .addUris(URI.newBuilder().setValue(configUri))
                  .setValue(
                      "export MESOS_NATIVE_JAVA_LIBRARY=" + javaLibPath +
                          " && /bin/cp $MESOS_SANDBOX/storm.yaml conf && /usr/bin/python bin/storm " +
                          "supervisor storm.mesos.MesosSupervisor -c storm.log.dir=$MESOS_SANDBOX/logs" + extraConfig))
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
                  .setValue("cp storm.yaml storm-mesos*/conf && cd storm-mesos* && python bin/storm " +
                      "supervisor storm.mesos.MesosSupervisor" + extraConfig));
        }

        LOG.info("Launching task with Mesos Executor data: <"
            + executorDataStr + ">");
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

        LOG.debug("Launching task: " + task.toString());

        toLaunch.get(id).add(new LaunchTask(task, newOffer));
      }

      if (usingExistingOffer) {
        _driver.killTask(taskId);
      }
    }
  }

  private FrameworkInfo.Builder createFrameworkBuilder() throws IOException {

    String id = (String) _state.get(FRAMEWORK_ID);
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

    if (id != null) {
      finfo.setId(FrameworkID.newBuilder().setValue(id).build());
    }

    return finfo;
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
          credentialBuilder.setSecret(ByteString.readFrom(new FileInputStream(secretFilename)));
        } catch (FileNotFoundException ex) {
          LOG.error("Mesos authentication secret file was not found", ex);
          throw new RuntimeException(ex);
        } catch (IOException ex) {
          LOG.error("Error reading Mesos authentication secret file", ex);
          throw new RuntimeException(ex);
        }
      }
      credential = credentialBuilder.build();
    }
    return credential;
  }

}
