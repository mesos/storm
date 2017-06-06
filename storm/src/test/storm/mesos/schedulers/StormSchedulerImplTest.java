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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static storm.mesos.TestUtils.buildOffer;
import static storm.mesos.TestUtils.buildOfferWithPorts;

@RunWith(MockitoJUnitRunner.class)
public class StormSchedulerImplTest {

  @Spy
  private StormSchedulerImpl stormSchedulerImpl;
  private Map<String, MesosWorkerSlot> mesosWorkerSlotMap;

  private Topologies topologies;
  private RotatingMap<OfferID, Offer> rotatingMap;
  private Set<String> topologiesMissingAssignments;
  private Map<String, TopologyDetails> topologyMap;
  private Collection<SupervisorDetails> existingSupervisors;
  private final String sampleTopologyId = "test-topology1-65-1442255385";
  private final String sampleHost = "host1.example.org";
  private final int samplePort = 3100;

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
    mesosWorkerSlotMap = (HashMap<String, MesosWorkerSlot>) (Whitebox.getInternalState(stormSchedulerImpl, "mesosWorkerSlotMap"));
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
    stormSchedulerImpl = new StormSchedulerImpl();
    Map<String, Object> mesosStormConf = new HashMap<>();
    stormSchedulerImpl.prepare(mesosStormConf);

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
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 0);


    /* Offer with no cpu but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", sampleHost, 0.0, 1000, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with no memory but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", sampleHost, 0.0, 1000, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with just enough ports, memory and cpu */
    /* Case 1 - Supervisor exists for topology test-topology1-65-1442255385 on the host */
    /* XXX(erikdw): Intentionally disabled until we fix https://github.com/mesos/storm/issues/160
    offer = buildOfferWithPorts("offer1", sampleHost, 0.1, 200, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(1, workerSlotsAvailableForScheduling.size());
    */

    /* Case 2 - Supervisor does not exists for topology test-topology1-65-1442255385 on the host */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 0.1, 200, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(0, workerSlotsAvailableForScheduling.size());

    /* Case 3 - Supervisor exists for topology test-topology1-65-1442255385 on the host & offer has additional resources for supervisor */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 0.1 + MesosCommon.DEFAULT_EXECUTOR_CPU, 200 + MesosCommon.DEFAULT_EXECUTOR_MEM_MB,
                                3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(1, workerSlotsAvailableForScheduling.size());

    /* Test default values for worker cpu and memory - This is to make sure that we account for default worker cpu and memory when the user does not pass MesosCommon.DEFAULT_WORKER_CPU && MesosCommon.DEFAULT_WORKER_MEM  */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    TopologyDetails topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 1);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(1, workerSlotsAvailableForScheduling.size());


    /* More than 1 worker slot is required - Plenty of memory & cpu is available, only two ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(2, workerSlotsAvailableForScheduling.size());

    /* More than 1 worker slot is required - Plenty of ports & cpu is available, but memory is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                2 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(2, workerSlotsAvailableForScheduling.size());

    /* More than 1 worker slot is required - Plenty of ports & memory are available, but cpu is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 2 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 100);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(2, workerSlotsAvailableForScheduling.size());

    /* 10 worker slots are required - Plenty of cpu, memory & ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.example.org", 20 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                20 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, samplePort, samplePort + 100);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(10, workerSlotsAvailableForScheduling.size());
  }

  private void addToRotatingMap(List<Offer> offers) {
     for (Offer offer:offers) {
       rotatingMap.put(offer.getId(), offer);
     }
  }

  @Test
  public void testAllSlotsAvailableForSchedulingWithMultipleOffers() {
    List<WorkerSlot> workerSlotsAvailableForScheduling;
    Offer offer;
    TopologyDetails topologyDetails;
    final double DEFAULT_WORKER_CPU = MesosCommon.DEFAULT_WORKER_CPU;
    final double DEFAULT_EXECUTOR_CPU = MesosCommon.DEFAULT_EXECUTOR_CPU;
    final double DEFAULT_WORKER_MEM = MesosCommon.DEFAULT_WORKER_MEM_MB;
    final double DEFAULT_EXECUTOR_MEM = MesosCommon.DEFAULT_EXECUTOR_MEM_MB;
    final String sampleHost2 = "host2.example.org";

    /* 10 worker slots are available but offers are fragmented on one host */
    List<Offer> offers = new ArrayList<>();
    offers.add(buildOffer("offer1", sampleHost, 0, DEFAULT_EXECUTOR_MEM));
    offers.add(buildOffer("offer2", sampleHost, DEFAULT_EXECUTOR_CPU, 0));
    offers.add(buildOfferWithPorts("offer6", sampleHost, 0, 0, samplePort, samplePort + 5));
    offers.add(buildOffer("offer7", sampleHost, 4 * DEFAULT_WORKER_CPU, 0));
    offers.add(buildOffer("offer8", sampleHost, 0, 4 * DEFAULT_WORKER_MEM));

    offers.add(buildOffer("offer10", sampleHost2, DEFAULT_EXECUTOR_CPU + DEFAULT_WORKER_CPU, 0));
    offers.add(buildOffer("offer11", sampleHost2, 0, DEFAULT_EXECUTOR_MEM + DEFAULT_WORKER_MEM));
    offers.add(buildOfferWithPorts("offer13", sampleHost2, 0, 0, samplePort, samplePort));

    /*
     * XXX(erikdw): add a hacky fudge-factor of 0.01 for now, to allow the floating-point
     * tabulation logic in AggregatedOffers to succeed.  We will switch to fixed-point math
     * later to allow exact calculations to succeed.
     * See this issue for more info: https://github.com/mesos/storm/issues/161
     *
     * Once that is fixed we can remove this 0.01 fudge-factor line, and we should also
     * analyze other places in this project's tests that have y.x1 (e.g., 0.01, 0.91, etc.).
     * i.e.,
     *
     *  % git grep -E '\..1' | grep -v pom.xml
     *  storm/src/main/storm/mesos/util/MesosCommon.java:  public static final double MESOS_MIN_CPU = 0.01;
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    assertEquals(0.4f, TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.CPU), 0.01f);
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    assertEquals(100f, TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.MEM), 0.01f);
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    assertEquals(TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.CPU), 0.4f, 0.01f);
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    assertEquals(TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.MEM), 100f, 0.01f);
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    offer = TestUtils.buildOffer("O-H1-2", "h1", 3.21, 0);
     *  storm/src/test/storm/mesos/MesosNimbusTest.java:    offer = TestUtils.buildOffer("O-H2-2", "h2", 3.21, 0);
     *  storm/src/test/storm/mesos/schedulers/StormSchedulerImplTest.java:     * all of this project's tests that have y.x1 (e.g., 0.01, 0.91, etc.).
     *  storm/src/test/storm/mesos/schedulers/StormSchedulerImplTest.java:    offers.add(buildOffer("offer9", sampleHost, 0.01, 0));
     *  storm/src/test/storm/mesos/schedulers/StormSchedulerImplTest.java:    offers.add(buildOffer("offer12", sampleHost2, 0.01, 10));
     */
    offers.add(buildOffer("offer9", sampleHost, 0.01, 0));
    offers.add(buildOffer("offer12", sampleHost2, 0.01, 10));

    addToRotatingMap(offers);

    // sampleHost  - We have enough resources for 4 workers
    // sampleHost2 - We have enough resources for 1 worker
    topologyMap.clear();
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(5, workerSlotsAvailableForScheduling.size());

    // Scenario : Cpu & Mem are available for 5 workers but ports are available only for 3 workers.
    // Reduce the number of ports on sampleHost to 2
    offer = buildOfferWithPorts("offer6", sampleHost, 0, 0, samplePort, samplePort + 1);
    rotatingMap.put(offer.getId(), offer);
    // We now only have resources for 3 workers
    topologyMap.clear();
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 3);

    // Scenario:  Mem & Ports are available for 5 workers but cpu is available only for 3 workers.
    offer = buildOfferWithPorts("offer6", sampleHost, 0, 0, samplePort, samplePort + 5);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer7", sampleHost, 3 * DEFAULT_WORKER_CPU, 0);
    rotatingMap.put(offer.getId(), offer);

    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors,
                                                                                        new Topologies(topologyMap), topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 4);

    // Scenario:  Cpu & Ports are available for 5 workers but Mem is available only for 3 workers.
    offer = buildOfferWithPorts("offer6", sampleHost, 0, 0, samplePort, samplePort + 5);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer7", sampleHost, 4 * DEFAULT_WORKER_CPU, 0);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer8", sampleHost, 0, 2 * DEFAULT_WORKER_MEM);
    rotatingMap.put(offer.getId(), offer);

    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors,
                                                                                        new Topologies(topologyMap), topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 3);

    // Scenario:  Mem, Cpu & Ports are available for 20 workers.
    offers.clear();
    offers.add(buildOfferWithPorts("offer6", sampleHost, 0, 0, samplePort, samplePort + 10));
    offers.add(buildOffer("offer7", sampleHost, 10 * DEFAULT_WORKER_CPU, 0));
    offers.add(buildOffer("offer8", sampleHost, 0, 10 * DEFAULT_WORKER_MEM));

    offers.add(buildOffer("offer3", sampleHost2, 10 * DEFAULT_WORKER_CPU + DEFAULT_EXECUTOR_CPU, 0));
    offers.add(buildOffer("offer4", sampleHost2, 0, 10 * DEFAULT_WORKER_MEM + DEFAULT_EXECUTOR_MEM));
    offers.add(buildOfferWithPorts("offer9", sampleHost2, 0, 0, samplePort, samplePort + 10));
    addToRotatingMap(offers);

    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors,
                                                                                        new Topologies(topologyMap), topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 10);
  }

  @Test
  public void testWorkerSpreadAcrossHosts() {
    List<WorkerSlot> workerSlotsAvailableForScheduling;
    TopologyDetails topologyDetails;
    final double DEFAULT_WORKER_CPU = MesosCommon.DEFAULT_WORKER_CPU;
    final double DEFAULT_EXECUTOR_CPU = MesosCommon.DEFAULT_EXECUTOR_CPU;
    final double DEFAULT_WORKER_MEM = MesosCommon.DEFAULT_WORKER_MEM_MB;
    final double DEFAULT_EXECUTOR_MEM = MesosCommon.DEFAULT_EXECUTOR_MEM_MB;
    final String hostName = "host";

    topologyMap.clear();
    topologyDetails = TestUtils.constructTopologyDetails(sampleTopologyId, 10);
    topologyMap.put(sampleTopologyId, topologyDetails);
    stormSchedulerImpl.prepare(topologyDetails.getConf());

    /* 10 worker slots are available but offers are fragmented on one host */
    List<Offer> offers = new ArrayList<>();
    offers.add(buildOfferWithPorts("offer1", "0", 10 * DEFAULT_WORKER_CPU + DEFAULT_WORKER_CPU, 10 * DEFAULT_WORKER_MEM + DEFAULT_EXECUTOR_MEM, samplePort, samplePort + 1000));
    offers.add(buildOfferWithPorts("offer2", "1", 10 * DEFAULT_WORKER_CPU + DEFAULT_WORKER_CPU, 10 * DEFAULT_WORKER_MEM + DEFAULT_EXECUTOR_MEM, samplePort, samplePort + 1000));
    offers.add(buildOfferWithPorts("offer3", "2", 1 * DEFAULT_WORKER_CPU + DEFAULT_WORKER_CPU, 10 * DEFAULT_WORKER_MEM + DEFAULT_EXECUTOR_MEM, samplePort, samplePort + 1000));
    offers.add(buildOfferWithPorts("offer4", "3", 10 * DEFAULT_WORKER_CPU + DEFAULT_WORKER_CPU, 10 * DEFAULT_WORKER_MEM + DEFAULT_EXECUTOR_MEM, samplePort, samplePort + 1000));
    addToRotatingMap(offers);


    // Make sure that the obtained worker slots are evenly spread across the available resources
    workerSlotsAvailableForScheduling = stormSchedulerImpl.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);

    Integer[] expectedWorkerCountPerHost = {3, 3, 1, 3};

    Integer[] actualWorkerCountPerHost = {0, 0, 0, 0};
    for (WorkerSlot workerSlot : workerSlotsAvailableForScheduling) {
      MesosWorkerSlot mesosWorkerSlot = (MesosWorkerSlot) workerSlot;
      String host = mesosWorkerSlot.getNodeId().split(":")[0];
      actualWorkerCountPerHost[Integer.parseInt(host)]++;
    }

    assertArrayEquals(expectedWorkerCountPerHost, actualWorkerCountPerHost);
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

    stormSchedulerImpl.schedule(topologies, spyCluster);

    Set<ExecutorDetails> assignedExecutors = spyCluster.getAssignmentById(sampleTopologyId).getExecutors();
    assertEquals(executorsToAssign, assignedExecutors);
  }

  @Test
  public void testScheduleWithMultipleSlotsOnSameHost() {
    Cluster spyCluster = this.getSpyCluster(3, 3);
    stormSchedulerImpl.schedule(topologies, spyCluster);
    SchedulerAssignment schedulerAssignment = spyCluster.getAssignments()
                                                         .get(sampleTopologyId);
    Map<ExecutorDetails, WorkerSlot> executorDetailsWorkerSlotMap = schedulerAssignment.getExecutorToSlot();
    /* We expect the three unassigned executors to be spread
       across the three available worker slots */
    assertEquals(executorDetailsWorkerSlotMap.keySet().size(), 3);
    assertEquals(executorDetailsWorkerSlotMap.values().size(), 3);

    spyCluster = this.getSpyCluster(3, 6);
    stormSchedulerImpl.schedule(topologies, spyCluster);
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
