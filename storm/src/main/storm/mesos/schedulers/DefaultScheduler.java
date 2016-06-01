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
import storm.mesos.resources.OfferResources;
import storm.mesos.resources.RangeResource;
import storm.mesos.resources.ResourceEntries;
import storm.mesos.resources.ResourceNotAvailabeException;
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
  private final Logger log = LoggerFactory.getLogger(DefaultScheduler.class);
  private Map mesosStormConf;
  private final Map<String, MesosWorkerSlot> mesosWorkerSlotMap = new HashMap<>();

  @Override
  public void prepare(Map conf) {
    mesosStormConf = conf;
  }

  private List<MesosWorkerSlot> getMesosWorkerSlots(Map<String, OfferResources> offerResourcesPerNode,
                                                    Collection<String> nodesWithExistingSupervisors,
                                                    TopologyDetails topologyDetails) {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    List<MesosWorkerSlot> mesosWorkerSlots = new ArrayList<>();
    boolean slotFound = true;
    int slotsNeeded = topologyDetails.getNumWorkers();

    while (slotFound && slotsNeeded > 0) {
      slotFound = false;
      for (String currentNode : offerResourcesPerNode.keySet()) {
        OfferResources offerResources = offerResourcesPerNode.get(currentNode);

        boolean supervisorExists = nodesWithExistingSupervisors.contains(currentNode);

        if (!offerResources.isFit(mesosStormConf, topologyDetails, supervisorExists)) {
          log.info("{} is not a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}",
                   offerResources.toString(), topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
          continue;
        }

        log.info("{} is a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}", offerResources.toString(),
                 topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
        nodesWithExistingSupervisors.add(currentNode);
        MesosWorkerSlot mesosWorkerSlot;

        try {
          mesosWorkerSlot = SchedulerUtils.createMesosWorkerSlot(mesosStormConf, offerResources, topologyDetails, supervisorExists);
        } catch (ResourceNotAvailabeException rexp) {
          log.warn(rexp.getMessage());
          continue;
        }

        mesosWorkerSlots.add(mesosWorkerSlot);
        slotFound = true;
        if (--slotsNeeded == 0) {
          break;
        }
      }
    }

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
      log.info("Not Declining all offers that are currently buffered because no topologies need assignments");
      //offers.clear();
      return new ArrayList<>();
    }

    log.info("Topologies that need assignments: {}", topologiesMissingAssignments.toString());

    List<WorkerSlot> allSlots = new ArrayList<>();
    Map<String, OfferResources> offerResourcesPerNode = MesosCommon.getConsolidatedOfferResourcesPerNode(offers);

    for (String currentTopology : topologiesMissingAssignments) {
      TopologyDetails topologyDetails = topologies.getById(currentTopology);
      int slotsNeeded = topologyDetails.getNumWorkers();


      log.info("Trying to find {} slots for {}", slotsNeeded, topologyDetails.getId());
      if (slotsNeeded <= 0) {
        continue;
      }

      Set<String> nodesWithExistingSupervisors = new HashSet<>();
      for (String currentNode : offerResourcesPerNode.keySet()) {
        if (SchedulerUtils.supervisorExists(currentNode, existingSupervisors, currentTopology)) {
          nodesWithExistingSupervisors.add(currentNode);
        }
      }

      List<MesosWorkerSlot> mesosWorkerSlotList = getMesosWorkerSlots(offerResourcesPerNode, nodesWithExistingSupervisors, topologyDetails);
      for (MesosWorkerSlot mesosWorkerSlot : mesosWorkerSlotList) {
        String slotId = String.format("%s:%s", mesosWorkerSlot.getNodeId(), mesosWorkerSlot.getPort());
        mesosWorkerSlotMap.put(slotId, mesosWorkerSlot);
        allSlots.add(mesosWorkerSlot);
      }

      log.info("Number of available slots for {} : {}", topologyDetails.getId(), allSlots.size());
    }

    log.info("Number of available slots: {}", allSlots.size());
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
