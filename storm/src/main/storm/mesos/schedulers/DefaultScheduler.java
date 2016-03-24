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

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.MesosNimbus;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Default Scheduler used by mesos-storm framework.
 */
public class DefaultScheduler implements IScheduler, IMesosStormScheduler {
  private final Logger log = LoggerFactory.getLogger(MesosNimbus.class);
  private Map mesosStormConf;
  private final Map<String, MesosWorkerSlot> mesosWorkerSlotMap = new HashMap<>();

  @Override
  public void prepare(Map conf) {
    mesosStormConf = conf;
  }

  /*
   * Different topologies have different resource requirements in terms of cpu and memory. So when Mesos asks
   * this scheduler for a list of available worker slots, we create "MesosWorkerSlot" and store them into mesosWorkerSlotMap.
   * Notably, we return a list of MesosWorkerSlot objects, even though Storm is only aware of the WorkerSlot type.  However,
   * since a MesosWorkerSlot *is* a WorkerSlot (in the polymorphic sense), Storm treats the list as WorkerSlot objects.
   *
   * Note:
   * 1. "MesosWorkerSlot" is the same as WorkerSlot except that it is dedicated for a topology upon creation. This means that,
   * a MesosWorkerSlot belonging to one topology cannot be used to launch a worker belonging to a different topology.
   * 2. Please note that this method is called before schedule is invoked. We use this opportunity to assign the MesosWorkerSlot
   * to a specific topology and store the state in "mesosWorkerSlotMap". This way, when Storm later calls schedule, we can just
   * look up the "mesosWorkerSlotMap" for a list of available slots for the particular topology.
   * 3. Given MesosWorkerSlot extends WorkerSlot, we shouldn't have to really create a "mesosWorkerSlotMap". Instead, in the schedule
   * method, we could have just upcasted the "WorkerSlot" to "MesosWorkerSlot". But this is not currently possible because storm
   * passes a recreated version of WorkerSlot to schedule method instead of passing the WorkerSlot returned by this method as is.
    */
  @Override
  public List<WorkerSlot> allSlotsAvailableForScheduling(RotatingMap<Protos.OfferID, Protos.Offer> offers,
                                                         Collection<SupervisorDetails> existingSupervisors,
                                                         Topologies topologies, Set<String> topologiesMissingAssignments) {
    if (topologiesMissingAssignments.isEmpty()) {
      log.info("Declining all offers that are currently buffered because no topologies need assignments");
      offers.clear();
      return new ArrayList<>();
    }

    log.info("Topologies that need assignments: {}", topologiesMissingAssignments.toString());

    // Decline those offers that cannot be used for any of the topologies that need assignments.
    for (Protos.Offer offer : offers.newestValues()) {
      boolean isOfferUseful = false;
      for (String currentTopology : topologiesMissingAssignments) {
        boolean supervisorExists = SchedulerUtils.supervisorExists(offer.getHostname(), existingSupervisors, currentTopology);
        TopologyDetails topologyDetails = topologies.getById(currentTopology);
        if (SchedulerUtils.isFit(mesosStormConf, offer, topologyDetails, supervisorExists)) {
          isOfferUseful = true;
          break;
        }
      }
      if (!isOfferUseful) {
        log.info("Declining Offer {} because it does not fit any of the topologies that need assignments", offer.getId().getValue());
        offers.clearKey(offer.getId());
      }
    }

    List<WorkerSlot> allSlots = new ArrayList<>();

    Map<String, List<OfferResources>> offerResourcesListPerNode = SchedulerUtils.getOfferResourcesListPerNode(offers);

    for (String currentTopology : topologiesMissingAssignments) {
      TopologyDetails topologyDetails = topologies.getById(currentTopology);
      int slotsNeeded = topologyDetails.getNumWorkers();

      double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
      double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

      log.info("Trying to find {} slots for {}", slotsNeeded, topologyDetails.getId());
      if (slotsNeeded == 0) {
        continue;
      }

      Set<String> nodesWithExistingSupervisors = new HashSet<>();
      for (String currentNode : offerResourcesListPerNode.keySet()) {
        if (SchedulerUtils.supervisorExists(currentNode, existingSupervisors, currentTopology)) {
          nodesWithExistingSupervisors.add(currentNode);
        }
      }

      Boolean slotFound;
      do {
        slotFound = false;
        for (String currentNode : offerResourcesListPerNode.keySet()) {
          boolean supervisorExists = nodesWithExistingSupervisors.contains(currentNode);
          if (slotsNeeded == 0) {
            break;
          }

          for (OfferResources resources : offerResourcesListPerNode.get(currentNode)) {
            boolean isFit = SchedulerUtils.isFit(mesosStormConf, resources, topologyDetails, supervisorExists);
            if (isFit) {
              log.info("{} is a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}", resources.toString(),
                       topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
              nodesWithExistingSupervisors.add(currentNode);
              MesosWorkerSlot mesosWorkerSlot = SchedulerUtils.createWorkerSlotFromOfferResources(mesosStormConf, resources, topologyDetails, supervisorExists);
              if (mesosWorkerSlot == null) {
                continue;
              }
              String slotId = mesosWorkerSlot.getNodeId() + ":" + mesosWorkerSlot.getPort();
              mesosWorkerSlotMap.put(slotId, mesosWorkerSlot);
              // Place this offer in the first bucket of the RotatingMap so that it is less likely to get rotated out
              offers.put(resources.getOfferId(), resources.getOffer());
              allSlots.add(mesosWorkerSlot);
              slotsNeeded--;
              slotFound = true;
            } else {
              log.info("{} is not a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}",
                       resources.toString(), topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
            }
          }
        }
      } while (slotFound == true && slotsNeeded > 0);
      log.info("Number of available slots for {} : {}", topologyDetails.getId(), allSlots.size());
    }

    log.info("Number of available slots: {}", allSlots.size());
    if (log.isDebugEnabled()) {
      for (WorkerSlot slot : allSlots) {
        log.debug("available slot: {}", slot);
      }
    }
    return allSlots;
  }


  Map<String, List<MesosWorkerSlot>> getMesosWorkerSlotPerTopology(List<WorkerSlot> workerSlots) {
    HashMap<String, List<MesosWorkerSlot>> perTopologySlotList = new HashMap<>();

    for (WorkerSlot workerSlot : workerSlots) {
      if (workerSlot.getNodeId() == null) {
        log.warn("Unexpected: Node id is null for worker slot while scheduling");
        continue;
      }
      MesosWorkerSlot mesosWorkerSlot = mesosWorkerSlotMap.get(workerSlot.getNodeId() +
                                                               ":" + String.valueOf(workerSlot.getPort()));

      String topologyId = mesosWorkerSlot.getTopologyId();
      if (perTopologySlotList.get(topologyId) == null) {
        perTopologySlotList.put(topologyId, new ArrayList<MesosWorkerSlot>());
      }
      perTopologySlotList.get(topologyId).add(mesosWorkerSlot);
    }

    return  perTopologySlotList;
  }

  List<List<ExecutorDetails>> executorsPerWorkerList(Cluster cluster, TopologyDetails topologyDetails, Integer slotsAvailable) {
    Collection<ExecutorDetails> executors = cluster.getUnassignedExecutors(topologyDetails);
    List<List<ExecutorDetails>> executorsPerWorkerList = new ArrayList<>();

    for (int i = 0; i < slotsAvailable; i++) {
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
      index = ++index % slotsAvailable;
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
    Map<String, List<MesosWorkerSlot>> perTopologySlotList = getMesosWorkerSlotPerTopology(workerSlots);

    // So far we know how many MesosSlots each of the topologies have got. Lets assign executors for each of them
    for (String topologyId : perTopologySlotList.keySet()) {
      TopologyDetails topologyDetails = topologies.getById(topologyId);
      List<MesosWorkerSlot> mesosWorkerSlots = perTopologySlotList.get(topologyId);

      int countSlotsRequested = topologyDetails.getNumWorkers();
      int countSlotsAssigned = cluster.getAssignedNumWorkers(topologyDetails);

      if (mesosWorkerSlots.size() == 0) {
        log.warn("No slots found for topology {} while scheduling", topologyId);
        continue;
      }

      int countSlotsAvailable = Math.min(mesosWorkerSlots.size(), (countSlotsRequested - countSlotsAssigned));

      List<List<ExecutorDetails>> executorsPerWorkerList = executorsPerWorkerList(cluster, topologyDetails, countSlotsAvailable);

      for (int i = 0; i < countSlotsAvailable; i++) {
        cluster.assign(mesosWorkerSlots.remove(0), topologyId, executorsPerWorkerList.remove(0));
      }
    }
    mesosWorkerSlotMap.clear();
  }
}
