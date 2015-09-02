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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static storm.mesos.PrettyProtobuf.offerMapToString;
import static storm.mesos.PrettyProtobuf.offerToString;
import static storm.mesos.PrettyProtobuf.taskInfoToString;
import static storm.mesos.PrettyProtobuf.taskStatusToString;

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
  public static final String CONF_MESOS_LOCAL_FILE_SERVER_PORT = "mesos.local.file.server.port";
  public static final String CONF_MESOS_LOCAL_FILE_SERVER_LOCATION = "mesos.local.file.server.location";
  public static final String CONF_MESOS_FRAMEWORK_NAME = "mesos.framework.name";

  public static final Logger LOG = Logger.getLogger(MesosNimbus.class);

  private static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  private final Object _offersLock = new Object();
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
  private java.net.URI _configUrl;
  private Map<TaskID, Offer> _usedOffers;
  private ScheduledExecutorService timerScheduler =
      Executors.newScheduledThreadPool(1);

  private static Set listIntoSet(List l) {
    if (l == null) {
      return null;
    } else {
        return new HashSet<String>(l);
    }
  }

  public static void main(String[] args) {
    backtype.storm.daemon.nimbus.launch(new MesosNimbus());
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
      _conf  = conf;
      _state = new LocalState(localDir);
      _allowedHosts = listIntoSet((List<String>) conf.get(CONF_MESOS_ALLOWED_HOSTS));
      _disallowedHosts = listIntoSet((List<String>) conf.get(CONF_MESOS_DISALLOWED_HOSTS));
      _scheduler = new NimbusScheduler();
      createLocalServerPort();
      setupHttpServer();
  }
  
  protected void createLocalServerPort() {
    Integer port = (Integer) _conf.get(CONF_MESOS_LOCAL_FILE_SERVER_PORT);
    LOG.debug("LocalFileServer configured to listen on port: " + port);
    _localFileServerPort = Optional.fromNullable(port); 
  }

  protected void setupHttpServer() throws Exception {
      _httpServer = new LocalFileServer();
      String localConfFolder = Optional.fromNullable((String) _conf.get(CONF_MESOS_LOCAL_FILE_SERVER_LOCATION)).or("conf");
      _configUrl = _httpServer.serveDir("/conf", localConfFolder, _localFileServerPort);
      
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
  private OfferResources getResources(Offer offer, double executorCpu, double executorMem, double cpu, double mem) {
    OfferResources resources = new OfferResources();

    double offerCpu = 0;
    double offerMem = 0;

    for (Resource r : offer.getResourcesList()) {
      if (r.getName().equals("cpus") && r.getScalar().getValue() > offerCpu) {
        offerCpu = r.getScalar().getValue();
        resources.cpuSlots = (int) Math.floor((offerCpu - executorCpu) / cpu);
      } else if (r.getName().equals("mem") && r.getScalar().getValue() > offerMem) {
        offerMem = r.getScalar().getValue();
        resources.memSlots = (int) Math.floor((offerMem - executorMem) / mem);
      }
    }

    int maxPorts = Math.min(resources.cpuSlots, resources.memSlots);

    for (Resource r : offer.getResourcesList()) {
      if (r.getName().equals("ports")) {
        for (Range range : r.getRanges().getRangeList()) {
          if (resources.ports.size() >= maxPorts) {
            break;
          } else {
            int start = (int) range.getBegin();
            int end = (int) range.getEnd();
            for (int p = start; p <= end; p++) {
              resources.ports.add(p);
              if (resources.ports.size() >= maxPorts) {
                break;
              }
            }
          }
        }
      }
    }

    LOG.debug("Offer: " + offerToString(offer));
    LOG.debug("Extracted resources: " + resources.toString());
    return resources;
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
   * @param offer Offer
   * @param existingSupervisors Supervisors which already placed on the node for the Offer
   * @param topologiesMissingAssignments Topology ids required assignment
   * @return Boolean value indicating supervisor existence
   */
  private boolean supervisorExists(
      Offer offer, Collection<SupervisorDetails> existingSupervisors, Set<String> topologiesMissingAssignments) {
    boolean alreadyExists = true;
    for (String topologyId: topologiesMissingAssignments) {
      String offerHost = offer.getHostname();
      boolean exists = false;
      for (SupervisorDetails d: existingSupervisors) {
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
            (_disallowedHosts != null && !_disallowedHosts.contains(hostname))
        ;
  }

  @Override
  public Collection<WorkerSlot> allSlotsAvailableForScheduling(
      Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
    synchronized (_offersLock) {
      LOG.debug("allSlotsAvailableForScheduling: Currently have " + _offers.size() + " offers buffered" +
          (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));
      if (!topologiesMissingAssignments.isEmpty()) {
        LOG.info("Topologies that need assignments: " + topologiesMissingAssignments.toString());
      } else {
        LOG.info("Declining offers because no topologies need assignments");
        _offers.clear();
        return new ArrayList<WorkerSlot>();
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

    List<WorkerSlot> allSlots = new ArrayList<WorkerSlot>();

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
        Resource r = getResourceRange(offer.getResourcesList(), port, port, "ports");
        if (r != null) return offer.getId();
      }
    }
    // Still haven't found the slot? Maybe it's an offer we already used.
    return null;
  }

  private Resource getResourceScalar(final List<Resource> offerResources,
                                     final double value,
                                     final String name) {
    for (Resource r : offerResources) {
      if (r.getType() == Type.SCALAR &&
          r.getName().equals(name) &&
          r.getScalar().getValue() >= value) {
        return r;
      }
    }
    return null;
  }

  private Resource getResourceRange(final List<Resource> offerResources,
                                    final long valueBegin, final long valueEnd,
                                    final String name) {
    for (Resource r : offerResources) {
      if (r.getType() == Type.RANGES && r.getName().equals(name)) {
        for (Range range : r.getRanges().getRangeList()) {
          if (valueBegin >= range.getBegin() && valueEnd <= range.getEnd()) {
            return r;
          }
        }
      }
    }
    return null;
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
          Offer offer = _offers.get(id);
          List<WorkerSlot> workerSlots = slotList.get(id);
          boolean usingExistingOffer = false;
          boolean subtractedExecutorResources = false;

          for (WorkerSlot slot : workerSlots) {
            TaskID taskId = TaskID.newBuilder()
                .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort()))
                .build();

            if (id == null || offer == null && _usedOffers.containsKey(taskId)) {
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
              TopologyDetails details = topologies.getById(topologyId);
              double workerCpu = MesosCommon.topologyWorkerCpu(_conf, details);
              double workerMem = MesosCommon.topologyWorkerMem(_conf, details);
              double executorCpu = MesosCommon.executorCpu(_conf);
              double executorMem = MesosCommon.executorMem(_conf);
              if (!subtractedExecutorResources) {
                workerCpu += executorCpu;
                workerMem += executorMem;
              }

              Map executorData = new HashMap();
              executorData.put(MesosCommon.SUPERVISOR_ID, MesosCommon.supervisorId(slot.getNodeId(), details.getId()));
              executorData.put(MesosCommon.ASSIGNMENT_ID, slot.getNodeId());

              // Determine roles for cpu, mem, ports
              String cpuRole = null;
              String memRole = null;
              String portsRole = null;

              Offer.Builder newBuilder = Offer.newBuilder();
              newBuilder.mergeFrom(offer);
              newBuilder.clearResources();

              Offer.Builder existingBuilder = Offer.newBuilder();
              existingBuilder.mergeFrom(offer);
              existingBuilder.clearResources();


              List<Resource> offerResources = offer.getResourcesList();
              List<Resource> reservedOfferResources = new ArrayList<>();
              for (Resource r : offerResources) {
                if (r.hasRole() && !r.getRole().equals("*")) {
                  reservedOfferResources.add(r);
                }
              }

              Resource cpuResource = getResourceScalar(reservedOfferResources, workerCpu, "cpus");
              if (cpuResource == null) {
                cpuResource = getResourceScalar(offerResources, workerCpu, "cpus");
              }
              Resource memResource = getResourceScalar(reservedOfferResources, workerMem, "mem");
              if (memResource == null) {
                memResource = getResourceScalar(offerResources, workerMem, "mem");
              }
              Resource portsResource = getResourceRange(reservedOfferResources, slot.getPort(), slot.getPort(), "ports");
              if (portsResource == null) {
                portsResource = getResourceRange(offerResources, slot.getPort(), slot.getPort(), "ports");
              }

              List<Resource> resourceList = new ArrayList<>();
              List<Resource> oldResourceList = new ArrayList<>();
              for (Resource r : offer.getResourcesList()) {
                Resource.Builder remnants = Resource.newBuilder();
                remnants.mergeFrom(r);
                Resource.Builder resource = Resource.newBuilder();
                resource.mergeFrom(r);

                if (r == cpuResource) {
                  cpuRole = r.getRole();
                  resource.setScalar(
                      Scalar.newBuilder()
                          .setValue(workerCpu));
                  resourceList.add(resource.build());
                  remnants.setScalar(
                      Scalar.newBuilder()
                          .setValue(r.getScalar().getValue() - workerCpu));
                } else if (r == memResource) {
                  memRole = r.getRole();
                  resource.setScalar(
                      Scalar.newBuilder()
                          .setValue(workerMem));
                  resourceList.add(resource.build());
                  remnants.setScalar(
                      Scalar.newBuilder()
                          .setValue(r.getScalar().getValue() - workerMem));
                } else if (r == portsResource) {
                  Ranges.Builder rb = Ranges.newBuilder();
                  for (Range range : r.getRanges().getRangeList()) {
                    if (slot.getPort() >= range.getBegin() && slot.getPort() <= range.getEnd()) {
                      portsRole = r.getRole();
                      resource.setRanges(
                          Ranges.newBuilder()
                              .addRange(
                                  Range.newBuilder()
                                      .setBegin(slot.getPort())
                                      .setEnd(slot.getPort())
                              ));
                      resourceList.add(resource.build());
                      rb.addRange(Range.newBuilder()
                          .setBegin(slot.getPort() + 1)
                          .setEnd(range.getEnd()));
                      break;
                    } else {
                      rb.addRange(range);
                    }
                  }
                }
                oldResourceList.add(remnants.build());
              }
              newBuilder.addAllResources(resourceList);
              Offer newOffer = newBuilder.build();
              Offer remainingOffer = existingBuilder.addAllResources(oldResourceList).build();

              // Update the remaining offer list
              _offers.put(id, remainingOffer);

              if (cpuRole == null) cpuRole = "*";
              if (memRole == null) memRole = "*";
              if (portsRole == null) portsRole = "*";

              String configUri;
              try {
                configUri = new URL(_configUrl.toURL(),
                    _configUrl.getPath() + "/storm.yaml").toString();
              } catch (MalformedURLException e) {
                throw new RuntimeException(e);
              }

              if (!subtractedExecutorResources) {
                workerCpu -= executorCpu;
                workerMem -= executorMem;
                subtractedExecutorResources = true;
              }

              String executorDataStr = JSONValue.toJSONString(executorData);
              LOG.info("Launching task with Mesos Executor data: <" + executorDataStr + ">");
              final String executorURI = (String) _conf.get(CONF_EXECUTOR_URI);
              TaskInfo task = TaskInfo.newBuilder()
                  .setName("worker " + slot.getNodeId() + ":" + slot.getPort())
                  .setTaskId(taskId)
                  .setSlaveId(offer.getSlaveId())
                  .setExecutor(ExecutorInfo.newBuilder()
                          .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
                          .setData(ByteString.copyFromUtf8(executorDataStr))
                          .setCommand(CommandInfo.newBuilder()
                              .addUris(URI.newBuilder().setValue(configUri))
                              // Instead of downloading the executor package through mesos, we are downloading it here since sometimes the file name can contain
                              // additional characters at the end. For example, if we are downloading from hdfs the URI will have "?op=OPEN" at the end. This
                              // can fail the complete command. Hence, instead of relying on mesos to do the download for us, we are explicitly using wget to do
                              // that and use that to rename the file.
                              .setValue("wget -O storm-mesos.tgz " + executorURI + " && tar xvf storm-mesos.tgz && cp storm.yaml storm-mesos*/conf && cd storm-mesos* && python bin/storm " +
                                  "supervisor storm.mesos.MesosSupervisor"))
                          .addResources(Resource.newBuilder()
                              .setName("cpus")
                              .setType(Type.SCALAR)
                              .setScalar(Scalar.newBuilder().setValue(executorCpu))
                              .setRole(cpuRole))
                          .addResources(Resource.newBuilder()
                              .setName("mem")
                              .setType(Type.SCALAR)
                              .setScalar(Scalar.newBuilder().setValue(executorMem))
                              .setRole(memRole))
                  )
                  .addResources(Resource.newBuilder()
                      .setName("cpus")
                      .setType(Type.SCALAR)
                      .setScalar(Scalar.newBuilder().setValue(workerCpu))
                      .setRole(cpuRole))
                  .addResources(Resource.newBuilder()
                      .setName("mem")
                      .setType(Type.SCALAR)
                      .setScalar(Scalar.newBuilder().setValue(workerMem))
                      .setRole(memRole))
                  .addResources(Resource.newBuilder()
                      .setName("ports")
                      .setType(Type.RANGES)
                      .setRanges(Ranges.newBuilder()
                          .addRange(Range.newBuilder()
                              .setBegin(slot.getPort())
                              .setEnd(slot.getPort())))
                      .setRole(portsRole))
                  .build();

              toLaunch.get(id).add(new LaunchTask(task, newOffer));
            }

            if (usingExistingOffer) {
              _driver.killTask(taskId);
            }
          }
        }
      }

      for (OfferID id : toLaunch.keySet()) {
        List<LaunchTask> tasks = toLaunch.get(id);
        List<TaskInfo> launchList = new ArrayList<>();

        LOG.info("Launching tasks for offerId: " + id.getValue() + ":" + launchTaskListToString(tasks));
        for (LaunchTask t : tasks) {
          launchList.add(t.task);
          _usedOffers.put(t.task.getTaskId(), t.offer);
        }

        List<OfferID> launchOffer = new ArrayList<>();
        launchOffer.add(id);
        _driver.launchTasks(launchOffer, launchList);
        _offers.remove(id);
      }
    }
  }

  private static class OfferResources {
    int cpuSlots = 0;
    int memSlots = 0;
    List<Integer> ports = new ArrayList<Integer>();

    @Override
    public String toString() {
      return ToStringBuilder.reflectionToString(this);
    }
  }

  public class NimbusScheduler implements Scheduler {
    private CountDownLatch _registeredLatch = new CountDownLatch(1);

    public void waitUntilRegistered() throws InterruptedException {
        _registeredLatch.await();
    }

    @Override
    public void registered(final SchedulerDriver driver, FrameworkID id, MasterInfo masterInfo) {
      _driver = driver;
      try {
        _state.put(FRAMEWORK_ID, id.getValue());
      } catch (IOException e) {
        LOG.error("Halting process...", e);
        Runtime.getRuntime().halt(1);
      }
      _offers = new RotatingMap<OfferID, Offer>(
          new RotatingMap.ExpiredCallback<OfferID, Offer>() {
            @Override
            public void expire(OfferID key, Offer val) {
              driver.declineOffer(val.getId());
            }
          }
      );

      Number lruCacheSize = (Number) _conf.get(CONF_MESOS_OFFER_LRU_CACHE_SIZE);
      if (lruCacheSize == null) lruCacheSize = 1000;
      final int intLruCacheSize = lruCacheSize.intValue();
      _usedOffers = Collections.synchronizedMap(new LinkedHashMap<TaskID, Offer>(intLruCacheSize + 1, .75F, true) {
        // This method is called just after a new entry has been added
        public boolean removeEldestEntry(Map.Entry eldest) {
          return size() > intLruCacheSize;
        }
      });

      Number offerExpired = (Number) _conf.get(Config.NIMBUS_MONITOR_FREQ_SECS);
      if (offerExpired == null) offerExpired = 60;
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
      }, 0, 2000 * offerExpired.intValue());

      // Completed registration, let anything waiting for us to do so continue
      _registeredLatch.countDown();
    }

    @Override
    public void reregistered(SchedulerDriver sd, MasterInfo info) {
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
    }

    @Override
    public void error(SchedulerDriver driver, String msg) {
      LOG.error("Received fatal error \nmsg:" + msg + "\nHalting process...");
      try {
        _httpServer.shutdown();
      } catch (Exception e) {
        // Swallow. Nothing we can do about it now.
      }
      Runtime.getRuntime().halt(2);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
      synchronized (_offersLock) {
        LOG.debug("resourceOffers: Currently have " + _offers.size() + " offers buffered" +
            (_offers.size() > 0 ? (":" + offerMapToString(_offers)) : ""));
        for (Offer offer : offers) {
          if (_offers != null && isHostAccepted(offer.getHostname())) {
            LOG.debug("resourceOffers: Recording offer from host: " + offer.getHostname() + ", offerId: " + offer.getId().getValue());
            _offers.put(offer.getId(), offer);
          } else {
            LOG.debug("resourceOffers: Declining offer from host: " + offer.getHostname() + ", offerId: " + offer.getId().getValue());
            driver.declineOffer(offer.getId());
          }
        }
        LOG.debug("resourceOffers: After processing offers, now have " + _offers.size() + " offers buffered:" + offerMapToString(_offers));
      }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID id) {
      LOG.info("Offer rescinded. offerId: " + id.getValue());
      synchronized (_offersLock) {
        _offers.remove(id);
      }
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
      LOG.debug("Received status update: " + taskStatusToString(status));
      switch (status.getState()) {
        case TASK_FINISHED:
        case TASK_FAILED:
        case TASK_KILLED:
        case TASK_LOST:
          final TaskID taskId = status.getTaskId();
          timerScheduler.schedule(new Runnable() {
            @Override
            public void run() {
              _usedOffers.remove(taskId);
            }
          }, MesosCommon.getSuicideTimeout(_conf), TimeUnit.SECONDS);
          break;
        default:
          break;
      }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
    }

    @Override
    public void slaveLost(SchedulerDriver driver, SlaveID id) {
      LOG.warn("Lost slave id: " + id.getValue());
    }

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executor, SlaveID slave, int status) {
      LOG.warn("Mesos Executor lost: executor: " + executor.getValue() +
          " slave: " + slave.getValue() + " status: " + status);
    }
  }
    private FrameworkInfo.Builder createFrameworkBuilder() throws IOException {
        
        String id = (String) _state.get(FRAMEWORK_ID);
        Number failoverTimeout = Optional.fromNullable((Number) _conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS)).or(3600);
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
        String secretFilename = Optional.fromNullable((String) _conf.get(CONF_MESOS_SECRET_FILE)).orNull();;
        
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
    
    
  private class LaunchTask {
    public final TaskInfo task;
    public final Offer offer;

    public LaunchTask(final TaskInfo task, final Offer offer) {
      this.task = task;
      this.offer = offer;
    }

    @Override
    public String toString() {
      return "Offer: " + offerToString(offer) + " TaskInfo: " + taskInfoToString(task);
    }
  }

  private static String launchTaskListToString(List<LaunchTask> launchTasks) {
    StringBuilder sb = new StringBuilder(1024);
    for (LaunchTask launchTask : launchTasks) {
      sb.append("\n");
      sb.append(launchTask.toString());
    }
    return sb.toString();
  }
}
