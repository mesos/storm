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
import com.google.protobuf.ByteString;
import org.apache.commons.lang.builder.ToStringBuilder;
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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MesosNimbus implements INimbus {
  public static final String CONF_EXECUTOR_URI = "mesos.executor.uri";
  public static final String CONF_MASTER_URL = "mesos.master.url";
  public static final String CONF_MASTER_FAILOVER_TIMEOUT_SECS = "mesos.master.failover.timeout.secs";
  public static final String CONF_MESOS_ALLOWED_HOSTS = "mesos.allowed.hosts";
  public static final String CONF_MESOS_DISALLOWED_HOSTS = "mesos.disallowed.hosts";
  public static final String CONF_MESOS_ROLE = "mesos.framework.role";
  public static final String CONF_MESOS_CHECKPOINT = "mesos.framework.checkpoint";
  public static final String CONF_MESOS_OFFER_LRU_CACHE_SIZE = "mesos.offer.lru.cache.size";

  public static final Logger LOG = Logger.getLogger(MesosNimbus.class);

  private static final String FRAMEWORK_ID = "FRAMEWORK_ID";
  private final Object OFFERS_LOCK = new Object();
  LocalState _state;
  NimbusScheduler _scheduler;
  volatile SchedulerDriver _driver;
  Timer _timer = new Timer();
  Map _conf;
  Set<String> _allowedHosts;
  Set<String> _disallowedHosts;
  private RotatingMap<OfferID, Offer> _offers;
  private LocalFileServer _httpServer;
  private java.net.URI _configUrl;
  private Map<TaskID, Offer> used_offers;
  private ScheduledExecutorService timerScheduler =
      Executors.newScheduledThreadPool(1);

  private static Set listIntoSet(List l) {
    if (l == null) {
      return null;
    } else return new HashSet<String>(l);
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
      _conf = conf;
      _state = new LocalState(localDir);
      String id = (String) _state.get(FRAMEWORK_ID);

      _allowedHosts = listIntoSet((List) _conf.get(CONF_MESOS_ALLOWED_HOSTS));
      _disallowedHosts = listIntoSet((List) _conf.get(CONF_MESOS_DISALLOWED_HOSTS));

      Semaphore initter = new Semaphore(0);
      _scheduler = new NimbusScheduler(initter);
      Number failoverTimeout = (Number) conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS);
      if (failoverTimeout == null) failoverTimeout = 3600;

      String role = (String) conf.get(CONF_MESOS_ROLE);
      if (role == null) role = new String("*");
      Boolean checkpoint = (Boolean) conf.get(CONF_MESOS_CHECKPOINT);
      if (checkpoint == null) checkpoint = new Boolean(false);

      FrameworkInfo.Builder finfo = FrameworkInfo.newBuilder()
          .setName("Storm!!!")
          .setFailoverTimeout(failoverTimeout.doubleValue())
          .setUser("")
          .setRole(role)
          .setCheckpoint(checkpoint);

      if (id != null) {
        finfo.setId(FrameworkID.newBuilder().setValue(id).build());
      }

      _httpServer = new LocalFileServer();
      _httpServer.prepare(conf);
      _configUrl = _httpServer.serveDir("/conf", "conf");
      LOG.info("Started serving config dir under " + _configUrl);

      MesosSchedulerDriver driver =
          new MesosSchedulerDriver(
              _scheduler,
              finfo.build(),
              (String) conf.get(CONF_MASTER_URL));

      driver.start();
      LOG.info("Waiting for scheduler to initialize...");
      initter.acquire();
      LOG.info("Scheduler initialized...");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private OfferResources getResources(Offer offer, double cpu, double mem) {
    OfferResources resources = new OfferResources();

    double offerCpu = 0;
    double offerMem = 0;

    for (Resource r : offer.getResourcesList()) {
      if (r.getName().equals("cpus") && r.getScalar().getValue() > offerCpu) {
        offerCpu = r.getScalar().getValue();
        resources.cpuSlots = (int) Math.floor(offerCpu / cpu);
      } else if (r.getName().equals("mem") && r.getScalar().getValue() > offerMem) {
        offerMem = r.getScalar().getValue();
        resources.memSlots = (int) Math.floor(offerMem / mem);
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

    LOG.debug("Offer: " + offer.toString());
    LOG.debug("Extracted resources: " + resources.toString());
    return resources;
  }

  private List<WorkerSlot> toSlots(Offer offer, double cpu, double mem) {
    OfferResources resources = getResources(offer, cpu, mem);

    List<WorkerSlot> ret = new ArrayList<WorkerSlot>();
    int availableSlots = Math.min(resources.cpuSlots, resources.memSlots);
    availableSlots = Math.min(availableSlots, resources.ports.size());
    for (int i = 0; i < availableSlots; i++) {
      ret.add(new WorkerSlot(offer.getHostname(), resources.ports.get(i)));
    }
    return ret;
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
    synchronized (OFFERS_LOCK) {
      LOG.info("Currently have " + _offers.size() + " offers buffered");
      if (!topologiesMissingAssignments.isEmpty()) {
        LOG.info("Topologies that need assignments: " + topologiesMissingAssignments.toString());
      } else {
        LOG.info("Declining offers because no topologies need assignments");
        _offers.clear();
      }
    }

    Double cpu = null;
    Double mem = null;
    // TODO: maybe this isn't the best approach. if a topology raises #cpus keeps failing,
    // it will mess up scheduling on this cluster permanently
    for (String id : topologiesMissingAssignments) {
      TopologyDetails details = topologies.getById(id);
      double tcpu = MesosCommon.topologyCpu(_conf, details);
      double tmem = MesosCommon.topologyMem(_conf, details);
      if (cpu == null || tcpu > cpu) {
        cpu = tcpu;
      }
      if (mem == null || tmem > mem) {
        mem = tmem;
      }
    }

    List<WorkerSlot> allSlots = new ArrayList<WorkerSlot>();

    if (cpu != null && mem != null) {
      synchronized (OFFERS_LOCK) {
        for (Offer offer : _offers.newestValues()) {
          allSlots.addAll(toSlots(offer, cpu, mem));
        }
      }
    }

    LOG.info("Number of available slots: " + allSlots.size());
    return allSlots;
  }

  private OfferID findOffer(WorkerSlot worker) {
    int port = worker.getPort();
    for (Offer offer : _offers.values()) {
      if (offer.getHostname().equals(worker.getNodeId())) {
        for (Resource r : offer.getResourcesList()) {
          if (r.getName().equals("ports")) {
            for (Range range : r.getRanges().getRangeList()) {
              if (port >= range.getBegin() && port <= range.getEnd()) {
                return offer.getId();
              }
            }
          }
        }
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
        return r;
      }
    }
    return null;
  }

  @Override
  public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slots) {
    synchronized (OFFERS_LOCK) {
      Map<OfferID, List<LaunchTask>> toLaunch = new HashMap<>();
      for (String topologyId : slots.keySet()) {
        for (WorkerSlot slot : slots.get(topologyId)) {
          OfferID id = findOffer(slot);
          Offer offer = _offers.get(id);
          Boolean usingExistingOffer = false;

          TaskID taskId = TaskID.newBuilder()
              .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort()))
              .build();

          if (id == null || offer == null && used_offers.containsKey(taskId)) {
            offer = used_offers.get(taskId);
            if (offer != null) {
              id = offer.getId();
              usingExistingOffer = true;
            }
          }
          if (id != null && offer != null) {
            if (!toLaunch.containsKey(id)) {
              toLaunch.put(id, new ArrayList());
            }
            TopologyDetails details = topologies.getById(topologyId);
            double cpu = MesosCommon.topologyCpu(_conf, details);
            double mem = MesosCommon.topologyMem(_conf, details);

            Map executorData = new HashMap();
            executorData.put(MesosCommon.SUPERVISOR_ID, slot.getNodeId() + "-" + details.getId());
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

            Resource cpuResource = getResourceScalar(reservedOfferResources, cpu, "cpus");
            if (cpuResource == null) {
              cpuResource = getResourceScalar(offerResources, cpu, "cpus");
            }
            Resource memResource = getResourceScalar(reservedOfferResources, mem, "mem");
            if (memResource == null) {
              memResource = getResourceScalar(offerResources, mem, "mem");
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
                        .setValue(cpu));
                resourceList.add(resource.build());
                remnants.setScalar(
                    Scalar.newBuilder()
                        .setValue(r.getScalar().getValue() - cpu));
              } else if (r == memResource) {
                memRole = r.getRole();
                resource.setScalar(
                    Scalar.newBuilder()
                        .setValue(mem));
                resourceList.add(resource.build());
                remnants.setScalar(
                    Scalar.newBuilder()
                        .setValue(r.getScalar().getValue() - mem));
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
                  "/storm.yaml").toString();
            } catch (MalformedURLException e) {
              throw new RuntimeException(e);
            }

            String executorDataStr = JSONValue.toJSONString(executorData);
            LOG.info("Launching task with executor data: <" + executorDataStr + ">");
            TaskInfo task = TaskInfo.newBuilder()
                .setName("worker " + slot.getNodeId() + ":" + slot.getPort())
                .setTaskId(taskId)
                .setSlaveId(offer.getSlaveId())
                .setExecutor(ExecutorInfo.newBuilder()
                    .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
                    .setData(ByteString.copyFromUtf8(executorDataStr))
                    .setCommand(CommandInfo.newBuilder()
                            .addUris(URI.newBuilder().setValue((String)_conf.get(CONF_EXECUTOR_URI)))
                            .addUris(URI.newBuilder().setValue(configUri))
                            .setValue("cp storm.yaml storm-mesos*/conf && cd storm-mesos* && python bin/storm supervisor storm.mesos.MesosSupervisor")
                    ))
                .addResources(Resource.newBuilder()
                    .setName("cpus")
                    .setType(Type.SCALAR)
                    .setScalar(Scalar.newBuilder().setValue(cpu))
                    .setRole(cpuRole))
                .addResources(Resource.newBuilder()
                    .setName("mem")
                    .setType(Type.SCALAR)
                    .setScalar(Scalar.newBuilder().setValue(mem))
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

            if (usingExistingOffer) {
              _driver.killTask(taskId);
            }
          }
        }
      }

      for (OfferID id : toLaunch.keySet()) {
        List<LaunchTask> tasks = toLaunch.get(id);
        List<TaskInfo> launchList = new ArrayList<>();

        LOG.info("Launching tasks for offer " + id.getValue() + "\n" + tasks.toString());
        for (LaunchTask t : tasks) {
          launchList.add(t.task);
          used_offers.put(t.task.getTaskId(), t.offer);
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

    Semaphore _initter;

    public NimbusScheduler(Semaphore initter) {
      _initter = initter;
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
      final int LRU_CACHE_SIZE = lruCacheSize.intValue();
      used_offers = Collections.synchronizedMap(new LinkedHashMap<TaskID, Offer>(LRU_CACHE_SIZE + 1, .75F, true) {
        // This method is called just after a new entry has been added
        public boolean removeEldestEntry(Map.Entry eldest) {
          return size() > LRU_CACHE_SIZE;
        }
      });

      Number offerExpired = (Number) _conf.get(Config.NIMBUS_MONITOR_FREQ_SECS);
      if (offerExpired == null) offerExpired = 60;
      _timer.scheduleAtFixedRate(new TimerTask() {
        @Override
        public void run() {
          try {
            synchronized (OFFERS_LOCK) {
              _offers.rotate();
            }
          } catch (Throwable t) {
            LOG.error("Received fatal error Halting process...", t);
            Runtime.getRuntime().halt(2);
          }
        }
      }, 0, 2000 * offerExpired.intValue());
      _initter.release();
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
      synchronized (OFFERS_LOCK) {
        _offers.clear();
        for (Offer offer : offers) {
          if (_offers != null && isHostAccepted(offer.getHostname())) {
            _offers.put(offer.getId(), offer);
          } else {
            driver.declineOffer(offer.getId());
          }
        }
      }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, OfferID id) {
      synchronized (OFFERS_LOCK) {
        _offers.remove(id);
      }
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
      LOG.info("Received status update: " + status.toString());
      switch (status.getState()) {
        case TASK_FINISHED:
        case TASK_FAILED:
        case TASK_KILLED:
        case TASK_LOST:
          final TaskID taskId = status.getTaskId();
          timerScheduler.schedule(new Runnable() {
            @Override
            public void run() {
              used_offers.remove(taskId);
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
      LOG.info("Lost slave: " + id.toString());
    }

    @Override
    public void executorLost(SchedulerDriver driver, ExecutorID executor, SlaveID slave, int status) {
      LOG.info("Executor lost: executor=" + executor + " slave=" + slave);
    }
  }

  private class LaunchTask {
    public final TaskInfo task;
    public final Offer offer;

    public LaunchTask(final TaskInfo task, final Offer offer) {
      this.task = task;
      this.offer = offer;
    }
  }
}
