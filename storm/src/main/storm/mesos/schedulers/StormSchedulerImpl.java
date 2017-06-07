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
package storm.mesos.schedulers;

import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos;
<<<<<<< HEAD
=======
import org.apache.mesos.SchedulerDriver;
>>>>>>> Stop accumulating offers in a RotatingMap, instead suppress offers when we don't need them and revive offers when we do need them.
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.IScheduler;
import org.apache.storm.scheduler.SchedulerAssignment;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ResourceNotAvailableException;
import storm.mesos.util.MesosCommon;

import static storm.mesos.util.PrettyProtobuf.offerMapKeySetToString;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

/**
 *  Default Scheduler used by mesos-storm framework.
 */
public class StormSchedulerImpl implements IScheduler, IMesosStormScheduler {
  private final Logger log = LoggerFactory.getLogger(StormSchedulerImpl.class);
  private Map mesosStormConf;
  private final Map<String, MesosWorkerSlot> mesosWorkerSlotMap = new HashMap<>();
  private volatile boolean offersSuppressed = false;
  private SchedulerDriver driver;

  private StormSchedulerImpl() {
    // We make this constructor private so that calling it results in a compile time error
  }

  public StormSchedulerImpl(final SchedulerDriver driver) {
    this.driver = driver;
  }

  @Override
  public void prepare(Map conf) {
    mesosStormConf = conf;
  }

  private List<MesosWorkerSlot> getMesosWorkerSlots(Map<String, AggregatedOffers> aggregatedOffersPerNode,
                                                    Collection<String> nodesWithExistingSupervisors,
                                                    TopologyDetails topologyDetails) {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);
    int requestedWorkerMemInt = (int) requestedWorkerMem;

    List<MesosWorkerSlot> mesosWorkerSlots = new ArrayList<>();
    boolean slotFound = false;
    int slotsNeeded = topologyDetails.getNumWorkers();

    /* XXX(erikdw): For now we clear out our knowledge of pre-existing supervisors while searching for slots
     * for this topology, to make the behavior of allSlotsAvailableForScheduling() mimic that of assignSlots().
     *
     * See this issue: https://github.com/mesos/storm/issues/160
     *
     * Until that issue is fixed, we must not discount the resources used by pre-existing supervisors.
     * Otherwise we will under-represent the resources needed as compared to what the more ignorant
     * assignSlots() will believe is needed, and thus may prevent MesosWorkerSlots from actually being
     * used.  i.e., assignSlots() doesn't know if supervisors already exist, since it doesn't receive the
     * existingSupervisors input parameter that allSlotsAvailableForScheduling() does.
     */
    nodesWithExistingSupervisors.clear();

    do {
      slotFound = false;
      for (String currentNode : aggregatedOffersPerNode.keySet()) {
        AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(currentNode);

        boolean supervisorExists = nodesWithExistingSupervisors.contains(currentNode);

        if (!aggregatedOffers.isFit(mesosStormConf, topologyDetails, supervisorExists)) {
          log.info("{} with requestedWorkerCpu {} and requestedWorkerMem {} does not fit onto {} with resources {}",
                   topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMemInt, aggregatedOffers.getHostname(), aggregatedOffers.toString());
          continue;
        }

        log.info("{} with requestedWorkerCpu {} and requestedWorkerMem {} does fit onto {} with resources {}",
                 topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMemInt, aggregatedOffers.getHostname(), aggregatedOffers.toString());
        MesosWorkerSlot mesosWorkerSlot;
        try {
          mesosWorkerSlot = SchedulerUtils.createMesosWorkerSlot(mesosStormConf, aggregatedOffers, topologyDetails, supervisorExists);
        } catch (ResourceNotAvailableException rexp) {
          log.warn(rexp.getMessage());
          continue;
        }

        nodesWithExistingSupervisors.add(currentNode);
        mesosWorkerSlots.add(mesosWorkerSlot);
        slotFound = true;
        if (--slotsNeeded == 0) {
          break;
        }
      }
    } while (slotFound && slotsNeeded > 0);

    return mesosWorkerSlots;
  }
  /*
   * Different topologies have different resource requirements in terms of cpu and memory. So when Mesos asks
   * this scheduler for a list of available worker slots, we create "MesosWorkerSlot" and store them into mesosWorkerSlotMap.
   * Notably, we return a list of MesosWorkerSlot objects, even though Storm is only aware of the WorkerSlot type.  However,
   * since a MesosWorkerSlot *is* a WorkerSlot (in the polymorphic sense), Storm treats the list as WorkerSlot objects.
   *
   * Note:
   * 1. "MesosWorkerSlot" is the same as WorkerSlot except that it is dedicated for a topology upon creation. This means that,
   *    a MesosWorkerSlot belonging to one topology cannot be used to launch a worker belonging to a different topology.
   * 2. Please note that this method is called before schedule is invoked. We use this opportunity to assign the MesosWorkerSlot
   *    to a specific topology and store the state in "mesosWorkerSlotMap". This way, when Storm later calls schedule, we can just
   *    look up the "mesosWorkerSlotMap" for a list of available slots for the particular topology.
   * 3. Given MesosWorkerSlot extends WorkerSlot, we shouldn't have to really create a "mesosWorkerSlotMap". Instead, in the schedule
   *    method, we could have just upcasted the "WorkerSlot" to "MesosWorkerSlot". But this is not currently possible because storm
   *    passes a recreated version of WorkerSlot to schedule method instead of passing the WorkerSlot returned by this method as is.
    */
  @Override
  public List<WorkerSlot> allSlotsAvailableForScheduling(Map<Protos.OfferID, Protos.Offer> offers,
                                                         Collection<SupervisorDetails> existingSupervisors,
                                                         Topologies topologies, Set<String> topologiesMissingAssignments) {
    if (topologiesMissingAssignments.isEmpty()) {
      if (!offers.isEmpty()) {
        log.info("Declining all offers that are currently buffered because no topologies need assignments. Declined offer ids: {}", offerMapKeySetToString(offers));
        for (Protos.OfferID offerId : offers.keySet()) {
          driver.declineOffer(offerId);
        }
        offers.clear();
      }
      if (!offersSuppressed) {
        log.info("Suppressing offers since we don't have any topologies that need assignments.");
        driver.suppressOffers();
        offersSuppressed = true;
      }
      return new ArrayList<>();
    }

    log.info("Topologies that need assignments: {}", topologiesMissingAssignments.toString());

    if (offers.isEmpty()) {
      if (offersSuppressed) {
        log.info("There are no offers available to make slots on, so we are reviving suppressed offers since we now have topologies " +
                 "that need assignments.");
        driver.reviveOffers();
        offersSuppressed = false;
      }
      // Note: We still have the offersLock at this point, so we return the empty ArrayList so that we can release the lock and acquire new offers
      return new ArrayList<>();
    }

    List<WorkerSlot> allSlots = new ArrayList<>();
    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(offers);

    for (String currentTopology : topologiesMissingAssignments) {
      TopologyDetails topologyDetails = topologies.getById(currentTopology);
      int slotsNeeded = topologyDetails.getNumWorkers();

      log.info("Trying to find {} slots for {}", slotsNeeded, topologyDetails.getId());
      if (slotsNeeded <= 0) {
        continue;
      }

      Set<String> nodesWithExistingSupervisors = new HashSet<>();
      for (String currentNode : aggregatedOffersPerNode.keySet()) {
        if (SchedulerUtils.supervisorExists(currentNode, existingSupervisors, currentTopology)) {
          nodesWithExistingSupervisors.add(currentNode);
        }
      }

      List<MesosWorkerSlot> mesosWorkerSlotList = getMesosWorkerSlots(aggregatedOffersPerNode, nodesWithExistingSupervisors, topologyDetails);
      for (MesosWorkerSlot mesosWorkerSlot : mesosWorkerSlotList) {
        String slotId = String.format("%s:%s", mesosWorkerSlot.getNodeId(), mesosWorkerSlot.getPort());
        mesosWorkerSlotMap.put(slotId, mesosWorkerSlot);
        allSlots.add(mesosWorkerSlot);
      }

      log.info("Number of available slots for {}: {}", topologyDetails.getId(), mesosWorkerSlotList.size());
    }

    List<String> slotsStrings = new ArrayList<String>();
    for (WorkerSlot slot : allSlots) {
      slotsStrings.add("" + slot.getNodeId() + ":" + slot.getPort());
    }
    log.info("allSlotsAvailableForScheduling: {} available slots: [{}]", allSlots.size(), StringUtils.join(slotsStrings, ", "));
    return allSlots;
  }


  Map<String, List<MesosWorkerSlot>> getMesosWorkerSlotPerTopology(List<WorkerSlot> workerSlots) {
    HashMap<String, List<MesosWorkerSlot>> perTopologySlotList = new HashMap<>();

    for (WorkerSlot workerSlot : workerSlots) {
      if (workerSlot.getNodeId() == null) {
        log.warn("Unexpected: Node id is null for worker slot while scheduling");
        continue;
      }
      MesosWorkerSlot mesosWorkerSlot = mesosWorkerSlotMap.get(String.format("%s:%d",
                                                                             workerSlot.getNodeId(),
                                                                             workerSlot.getPort()));

      String topologyId = mesosWorkerSlot.getTopologyId();
      if (perTopologySlotList.get(topologyId) == null) {
        perTopologySlotList.put(topologyId, new ArrayList<MesosWorkerSlot>());
      }
      perTopologySlotList.get(topologyId).add(mesosWorkerSlot);
    }

    return  perTopologySlotList;
  }

  List<List<ExecutorDetails>> executorsPerWorkerList(Cluster cluster, TopologyDetails topologyDetails,
                                                     int slotsRequested, int slotsAssigned, int slotsAvailable) {
    Collection<ExecutorDetails> executors = cluster.getUnassignedExecutors(topologyDetails);
    String topologyId = topologyDetails.getId();

    // Check if we don't actually need to schedule any executors because all requested slots are assigned already.
    if (slotsRequested == slotsAssigned) {
      if (executors.isEmpty()) {
        // TODO: print executors list cleanly in a single line
        String msg = String.format("executorsPerWorkerList: for %s, slotsRequested: %d == slotsAssigned: %d, BUT there are " +
                     "unassigned executors which is nonsensical",
                     topologyId, slotsRequested, slotsAssigned);
        log.error(msg);
        throw new RuntimeException(msg);
      }
      log.debug("executorsPerWorkerList: for {}, slotsRequested: {} == slotsAssigned: {}, so no need to schedule any executors",
                topologyId, slotsRequested, slotsAssigned);
      return null;
    }

    int slotsToUse = 0;

    // If there are not any unassigned executors, we need to re-distribute all currently assigned executors across workers
    if (executors.isEmpty()) {
      if (slotsAssigned < slotsAvailable) {
        log.info("All executors are already assigned for {}, but only onto {} slots. Redistributing all assigned executors to new set of {} slots.",
                 topologyId, slotsAssigned, slotsAvailable);
        SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyId);
        // Un-assign them
        int slotsFreed = schedulerAssignment.getSlots().size();
        cluster.freeSlots(schedulerAssignment.getSlots());
        log.info("executorsPerWorkerList: for {}, slotsAvailable: {}, slotsAssigned: {}, slotsFreed: {}",
                 topologyId, slotsAvailable, slotsAssigned, slotsFreed);
        executors = cluster.getUnassignedExecutors(topologyDetails);
        slotsToUse = slotsAvailable;
      } else {
        log.info("All executors are already assigned for {}. Not going to redistribute work because slotsAvailable is {} and slotsAssigned is {}",
                 topologyId, slotsAvailable, slotsAssigned);
        return null;
      }
    } else {
      /*
       * Spread the unassigned executors onto however many available slots we can possibly use.
       * i.e., there might be more than we need.
       *
       * Note that this logic can lead to an imbalance of executors/worker between various workers.
       *
       * We propose to avoid such problems by having an option (perhaps on by default) which only will
       * ever schedule onto the exact requested number of workers.
       * See https://github.com/mesos/storm/issues/158
       * For now we just issue a warning when we detect such a situation.
       */
      int slotsNeeded = slotsRequested - slotsAssigned;
      // Just in case something strange happens, we don't want this to be negative
      slotsToUse = Math.max(Math.min(slotsNeeded, slotsAvailable), 0);
      // Notably, if slotsAssigned was 0, then this would be a full rebalance onto less workers than requested,
      // and hence wouldn't lead to an imbalance.
      if (slotsToUse + slotsAssigned < slotsRequested && slotsAssigned != 0) {
        log.warn("For {}, assigning {} storm executors onto {} new slots when we already have {} executors assigned to {} slots, " +
                 "this may lead to executor imbalance.",
                 topologyId, executors.size(), slotsToUse, cluster.getAssignmentById(topologyId).getExecutors().size(), slotsAssigned);
      }
    }

    List<String> executorsStrings = new ArrayList<String>();
    List<List<ExecutorDetails>> executorsPerWorkerList = new ArrayList<>();
    for (ExecutorDetails exec : executors) {
      executorsStrings.add(exec.toString());
    }
    String info = String.format("executorsPerWorkerList: available executors for %s: %s", topologyId, StringUtils.join(executorsStrings, ", "));
    for (int i = 0; i < slotsToUse; i++) {
      executorsPerWorkerList.add(new ArrayList<ExecutorDetails>());
    }

    List<ExecutorDetails> executorList = new ArrayList<>(executors);

    /* The goal of this scheduler is to mimic Storm's default version. Storm's default scheduler sorts the
     * executors by their id before spreading them across the available workers.
     */
    Collections.sort(executorList, new Comparator<ExecutorDetails>() {
      public int compare(ExecutorDetails e1, ExecutorDetails e2) {
        return e1.getStartTask() - e2.getStartTask();
      }
    });

    int index = -1;
    for (ExecutorDetails executorDetails : executorList) {
      index = ++index % slotsToUse;
      // log.info("executorsPerWorkerList -- adding {} to list at index {}", executorDetails.toString(), index);
      executorsPerWorkerList.get(index).add(executorDetails);
    }

    return executorsPerWorkerList;
  }

  /**
   * Schedule function looks in the "mesosWorkerSlotMap" to determine which topology owns the particular
   * WorkerSlot and assigns the executors accordingly.
   */
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    List<WorkerSlot> workerSlots = cluster.getAvailableSlots();
    String info = "Scheduling the following worker slots from cluster.getAvailableSlots: ";
    if (workerSlots.isEmpty()) {
      info += "[]";
    } else {
      List<String> workerSlotsStrings = new ArrayList<String>();
      for (WorkerSlot ws : workerSlots) {
        workerSlotsStrings.add(ws.toString());
      }
      info += String.format("[%s]", StringUtils.join(workerSlotsStrings, ", "));
    }
    log.info(info);

    Map<String, List<MesosWorkerSlot>> perTopologySlotList = getMesosWorkerSlotPerTopology(workerSlots);
    if (perTopologySlotList.isEmpty()) {
      log.info("No slots for any topologies, scheduling completed");
      return;
    }
    info = "Schedule the per-topology slots:";
    for (String topo : perTopologySlotList.keySet()) {
      List<String> mwsAssignments = new ArrayList<>();
      for (MesosWorkerSlot mws : perTopologySlotList.get(topo)) {
        mwsAssignments.add(mws.getNodeId() + ":" + mws.getPort());
      }
      info += String.format(" {%s, [%s]}", topo, StringUtils.join(mwsAssignments, ", "));
    }
    log.info(info);

    // So far we know how many MesosSlots each of the topologies have got. Let's assign executors for each of them
    for (String topologyId : perTopologySlotList.keySet()) {
      TopologyDetails topologyDetails = topologies.getById(topologyId);
      List<MesosWorkerSlot> mesosWorkerSlots = perTopologySlotList.get(topologyId);

      int slotsRequested = topologyDetails.getNumWorkers();
      int slotsAssigned = cluster.getAssignedNumWorkers(topologyDetails);
      int slotsAvailable = mesosWorkerSlots.size();

      if (slotsAvailable == 0) {
        log.warn("No slots found for topology {} while scheduling", topologyId);
        continue;
      }

      log.info("topologyId: {}, slotsRequested: {}, slotsAssigned: {}, slotsAvailable: {}", topologyId, slotsRequested, slotsAssigned, slotsAvailable);

      List<List<ExecutorDetails>> executorsPerWorkerList = executorsPerWorkerList(cluster, topologyDetails, slotsRequested, slotsAssigned, slotsAvailable);
      if (executorsPerWorkerList == null || executorsPerWorkerList.isEmpty()) {
        continue;
      }

      info = "schedule: Cluster assignment for " + topologyId + "."
           + " Requesting " + slotsRequested + " slots, with " + slotsAvailable + " slots available, and " + slotsAssigned + " currently assigned."
           + " Setting new assignment (node:port, executorsPerWorkerList) as: ";
      List<String> slotAssignmentStrings = new ArrayList<String>();
      ListIterator<List<ExecutorDetails>> iterator = executorsPerWorkerList.listIterator();
      while (iterator.hasNext()) {
        List<ExecutorDetails> executorsPerWorker = iterator.next();
        slotAssignmentStrings.add("(" + mesosWorkerSlots.get(0).getNodeId() + ":" + mesosWorkerSlots.get(0).getPort() + ", " + executorsPerWorker.toString() + ")");
        iterator.remove();
        cluster.assign(mesosWorkerSlots.remove(0), topologyId, executorsPerWorker);
      }
      if (slotsAvailable == 0) {
        info += "[]";
      } else {
        info += StringUtils.join(slotAssignmentStrings, ", ");
      }
      log.info(info);
    }
    mesosWorkerSlotMap.clear();
  }
}
