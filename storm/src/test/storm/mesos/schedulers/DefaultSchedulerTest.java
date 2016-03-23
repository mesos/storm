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

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SchedulerAssignmentImpl;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import storm.mesos.TestUtils;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static storm.mesos.TestUtils.buildOffer;
import static storm.mesos.TestUtils.buildOfferWithPorts;

@RunWith(MockitoJUnitRunner.class)
public class DefaultSchedulerTest {

  @Spy
  private DefaultScheduler defaultScheduler;
  private Map<String, MesosWorkerSlot> mesosWorkerSlotMap;

  private Topologies topologies;
  private RotatingMap<OfferID, Offer> rotatingMap;
  private Set<String> topologiesMissingAssignments;
  private Map<String, TopologyDetails> topologyMap;
  private Collection<SupervisorDetails> existingSupervisors;
  private final String sampleTopologyId = "test-topology1-65-1442255385";
  private final String sampleHost = "host1.east";
  private final int samplePort = 3100;



  private TopologyDetails constructTopologyDetails(String topologyName, int numWorkers) {
    Map<String, TopologyDetails> topologyConf1 = new HashMap<>();

    StormTopology stormTopology = new StormTopology();
    TopologyDetails topologyDetails= new TopologyDetails(topologyName, topologyConf1, stormTopology, numWorkers);

    return topologyDetails;
  }


  private Cluster getSpyCluster() {
    Map<String, SupervisorDetails> supervisors = new HashMap<>();
    Map<String, SchedulerAssignmentImpl> assignmentMap = new HashMap<>();

    for (SupervisorDetails supervisorDetails : existingSupervisors) {
      String nodeId = supervisorDetails.getHost();
      supervisors.put(nodeId, supervisorDetails);
    }

    return spy(new Cluster(null, supervisors, assignmentMap));
  }

  private void initializeMesosWorkerSlotMap(List<MesosWorkerSlot> mesosWorkerSlots) {
    mesosWorkerSlotMap = (HashMap<String, MesosWorkerSlot>) (Whitebox.getInternalState(defaultScheduler, "mesosWorkerSlotMap"));
    for (MesosWorkerSlot mesosWorkerSlot: mesosWorkerSlots) {
      mesosWorkerSlotMap.put(String.format("%s:%s", mesosWorkerSlot.getNodeId(), mesosWorkerSlot.getPort()), mesosWorkerSlot);
    }
  }
  
  private List<WorkerSlot> getWorkerSlotFromMesosWorkerSlot(List<MesosWorkerSlot> mesosWorkerSlotList) {
    List<WorkerSlot> workerSlotList = new ArrayList<>();
    for (WorkerSlot mesosWorkerSlot : mesosWorkerSlotList) {
      workerSlotList.add(new WorkerSlot(mesosWorkerSlot.getNodeId(), mesosWorkerSlot.getPort()));
    }
    return workerSlotList;
  }

  private Set<ExecutorDetails> generateExecutorDetailsSet(int count) {
    Set<ExecutorDetails> executorsToAssign = new HashSet<>();
    for (int i=0; i < count; i++) {
      executorsToAssign.add(new ExecutorDetails(i, i));
    }
    return executorsToAssign;
  }

  private List<MesosWorkerSlot> generateMesosWorkerSlots(int count) {
    List<MesosWorkerSlot> mesosWorkerSlots = new ArrayList<>();

    for (int i=0; i<count; i++) {
      mesosWorkerSlots.add(new MesosWorkerSlot(sampleHost, samplePort + i, sampleTopologyId));
    }
    return mesosWorkerSlots;
  }

  private Cluster getSpyCluster(int numWorkers, int numExecutors) {
    Cluster spyCluster = getSpyCluster();

    List<MesosWorkerSlot> mesosWorkerSlots = this.generateMesosWorkerSlots(numWorkers);
    initializeMesosWorkerSlotMap(mesosWorkerSlots);

    Set<ExecutorDetails> executorsToAssign = this.generateExecutorDetailsSet(numExecutors);
    List<WorkerSlot> workerSlotList = getWorkerSlotFromMesosWorkerSlot(mesosWorkerSlots);
    topologyMap.put(sampleTopologyId, TestUtils.constructTopologyDetails(sampleTopologyId, numWorkers, 0.1, 100));
    this.topologies = new Topologies(topologyMap);

    doReturn(workerSlotList).when(spyCluster).getAvailableSlots();
    doReturn(executorsToAssign).when(spyCluster).getUnassignedExecutors(any(TopologyDetails.class));

    return spyCluster;
  }

  private  Map<WorkerSlot, List<ExecutorDetails>> getworkerSlotExecutorDetailsMap(Map<ExecutorDetails, WorkerSlot> executorDetailsWorkerSlotMap) {
    Map<WorkerSlot, List<ExecutorDetails>> workerSlotExecutorDetailsMap = new HashMap<>();

    for (ExecutorDetails executorDetails : executorDetailsWorkerSlotMap.keySet()) {
      WorkerSlot workerSlot = executorDetailsWorkerSlotMap.get(executorDetails);
      if (!workerSlotExecutorDetailsMap.containsKey(workerSlot)) {
        workerSlotExecutorDetailsMap.put(workerSlot, new ArrayList<ExecutorDetails>());
      }
      workerSlotExecutorDetailsMap.get(workerSlot).add(executorDetails);
    }
    return workerSlotExecutorDetailsMap;
  }

  @Before
  public void initialize() {
    defaultScheduler = new DefaultScheduler();
    Map<String, Object> mesosStormConf = new HashMap<>();
    defaultScheduler.prepare(mesosStormConf);

    rotatingMap = new RotatingMap<>(2);

    topologiesMissingAssignments = new HashSet<>();
    topologiesMissingAssignments.add("test-topology1-65-1442255385");
    topologiesMissingAssignments.add("test-topology1-65-1442255385");

    existingSupervisors = new ArrayList<>();
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(sampleHost, "test-topology1-65-1442255385"), sampleHost, null, null));
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(sampleHost, "test-topology10-65-1442255385"), sampleHost, null, null));

    topologyMap = new HashMap<>();
    topologyMap.put(sampleTopologyId, TestUtils.constructTopologyDetails(sampleTopologyId, 1, 0.1, 100));
    topologies = new Topologies(topologyMap);

    mesosWorkerSlotMap = new HashMap<>();
  }

  @Test
  public void testAllSlotsAvailableForSchedulingWithOneOffer() {
    List<WorkerSlot> workerSlotsAvailableForScheduling;

    /* Offer with no ports but enough memory and cpu*/
    Offer offer = buildOffer("offer1", sampleHost, 10, 20000);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 0);


    /* Offer with no cpu but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", sampleHost, 0.0, 1000, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with no memory but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", sampleHost, 0.0, 1000, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with just enough ports, memory and cpu */
    /* Case 1 - Supervisor exists for topology test-topology1-65-1442255385 on the host */
    offer = buildOfferWithPorts("offer1", sampleHost, 0.1, 200, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 1);

    /* Case 2 - Supervisor does not exists for topology test-topology1-65-1442255385 on the host */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 0.1, 200, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 0);

    /* Case 3 - Supervisor exists for topology test-topology1-65-1442255385 on the host & offer has additional resources for supervisor */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 0.1 + MesosCommon.DEFAULT_EXECUTOR_CPU, 200 + MesosCommon.DEFAULT_EXECUTOR_MEM_MB,
                                3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 1);

    /* Test default values for worker cpu and memory - This is to make sure that we account for default worker cpu and memory when the user does not pass MesosCommon.DEFAULT_WORKER_CPU && MesosCommon.DEFAULT_WORKER_MEM  */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    TopologyDetails topologyDetails = constructTopologyDetails(sampleTopologyId, 1);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 1);


    /* More than 1 worker slot is required - Plenty of memory & cpu is available, only two ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* More than 1 worker slot is required - Plenty of ports & cpu is available, but memory is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                2 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* More than 1 worker slot is required - Plenty of ports & memory are available, but cpu is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 2 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 100);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* 10 worker slots are required - Plenty of cpu, memory & ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 20 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                20 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 100);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 10);
  }

  @Test
  public void testAllSlotsAvailableForSchedulingWithMultipleOffers() {
    List<WorkerSlot> workerSlotsAvailableForScheduling;
    Offer offer;
    TopologyDetails topologyDetails;

    /* 10 worker slots are available but offers are fragmented on one host */
    offer = buildOffer("offer1", sampleHost, 0, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer2", sampleHost, 10, 0);
    rotatingMap.put(offer.getId(), offer);
    String sampleHost2 = "host1.west";
    offer = buildOffer("offer3", sampleHost2, 0.01, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer4", sampleHost2, 0.1, 9000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer5", sampleHost2, 0.91, 9000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOfferWithPorts("offer6", sampleHost2, 5 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                5 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 5);
    rotatingMap.put(offer.getId(), offer);

    topologyMap.clear();
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    // Increase available cpu by a tiny fraction in order
    offer = buildOfferWithPorts("offer6", sampleHost, 5 * MesosCommon.DEFAULT_WORKER_CPU + 1.1 * MesosCommon.DEFAULT_EXECUTOR_CPU,
                                5 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 5);
    rotatingMap.put(offer.getId(), offer);

    topologyMap.clear();
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 5); // Note that by increasing the executor cpu by a fraction, we are able to get 5 worker slots as we expect


    topologyMap.clear();
    topologyDetails = constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 5);

    offer = buildOfferWithPorts("offer7", "host2.east", 3 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                3 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 5);
    rotatingMap.put(offer.getId(), offer);

    offer = buildOfferWithPorts("offer8", "host3.east", 100 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                100 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 10);
    rotatingMap.put(offer.getId(), offer);

    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors,
                                                                                        new Topologies(topologyMap), topologiesMissingAssignments);

    assertEquals(workerSlotsAvailableForScheduling.size(), 10);

    // Make sure that the obtained worker slots are evenly spread across the available resources
    Map<String, Integer> workerCountPerHostMap = new HashMap<>();

    for (WorkerSlot workerSlot : workerSlotsAvailableForScheduling) {
      Integer tmp = workerCountPerHostMap.get(workerSlot.getNodeId());
      if (tmp == null) {
        workerCountPerHostMap.put(workerSlot.getNodeId(), 1);
        continue;
      }
      workerCountPerHostMap.put(workerSlot.getNodeId(), tmp + 1);
    }

    List<Integer> expectedWorkerCountArray = Arrays.asList(3, 3, 4);
    List<Integer> actualWorkerCountArray = Arrays.asList(
                                                workerCountPerHostMap.get("host1.east"),
                                                workerCountPerHostMap.get("host2.east"),
                                                workerCountPerHostMap.get("host3.east"));

    Collections.sort(actualWorkerCountArray);
    assertEquals(expectedWorkerCountArray, actualWorkerCountArray);
  }

  @Test
  public void testScheduleWithOneWorkerSlot() {
    Cluster spyCluster = getSpyCluster();

    List<MesosWorkerSlot> mesosWorkerSlots = this.generateMesosWorkerSlots(1);
    initializeMesosWorkerSlotMap(mesosWorkerSlots);

    Set<ExecutorDetails> executorsToAssign = this.generateExecutorDetailsSet(4);
    List<WorkerSlot> workerSlotList = getWorkerSlotFromMesosWorkerSlot(mesosWorkerSlots);

    doReturn(workerSlotList).when(spyCluster).getAvailableSlots();
    doReturn(executorsToAssign).when(spyCluster).getUnassignedExecutors(any(TopologyDetails.class));

    defaultScheduler.schedule(topologies, spyCluster);

    Set<ExecutorDetails> assignedExecutors = spyCluster.getAssignmentById(sampleTopologyId).getExecutors();
    assertEquals(executorsToAssign, assignedExecutors);
  }

  @Test
  public void testScheduleWithMultipleSlotsOnSameHost() {
    Cluster spyCluster = this.getSpyCluster(3, 3);
    defaultScheduler.schedule(topologies, spyCluster);
    SchedulerAssignment schedulerAssignment = spyCluster.getAssignments()
                                                         .get(sampleTopologyId);
    Map<ExecutorDetails, WorkerSlot> executorDetailsWorkerSlotMap = schedulerAssignment.getExecutorToSlot();
    /* We expect the three unassigned executors to be spread
       across the three available worker slots */
    assertEquals(executorDetailsWorkerSlotMap.keySet().size(), 3);
    assertEquals(executorDetailsWorkerSlotMap.values().size(), 3);

    spyCluster = this.getSpyCluster(3, 6);
    defaultScheduler.schedule(topologies, spyCluster);
    executorDetailsWorkerSlotMap = spyCluster.getAssignments()
                                              .get(sampleTopologyId)
                                              .getExecutorToSlot();
    /* We expect all executors to be scheduled across the three
       available slots */
    assertEquals(executorDetailsWorkerSlotMap.keySet().size(), 6);
    int workerSlotsUsed = new HashSet<>(executorDetailsWorkerSlotMap.values()).size() ;
    assertEquals(workerSlotsUsed, 3);

    /* Lets make sure that the executors are evenly spread
       across the worker slots in a round robin fashion */
    schedulerAssignment = spyCluster.getAssignments()
                                                         .get(sampleTopologyId);
    executorDetailsWorkerSlotMap = schedulerAssignment.getExecutorToSlot();

    Map<WorkerSlot, List<ExecutorDetails>> workerSlotExecutorDetailsMap = this.getworkerSlotExecutorDetailsMap(executorDetailsWorkerSlotMap);

    for (WorkerSlot workerSlot : workerSlotExecutorDetailsMap.keySet()) {
      List<ExecutorDetails> executorDetails = workerSlotExecutorDetailsMap.get(workerSlot);
      assertEquals(3, Math.abs(executorDetails.get(0).getStartTask() - executorDetails.get(1).getEndTask()));
    }
  }
}
