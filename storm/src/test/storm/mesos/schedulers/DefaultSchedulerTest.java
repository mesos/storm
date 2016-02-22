/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package storm.mesos.schedulers;

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

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.apache.mesos.Protos.*;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import static storm.mesos.TestUtils.*;

public class DefaultSchedulerTest {

  @Test
  public void testSupervisorExists() throws Exception {
    DefaultScheduler defaultScheduler = new DefaultScheduler();
    Collection<SupervisorDetails> existingSupervisors = new ArrayList<>();
    String hostName = "host1.east";

    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName,"test-topology1-65-1442255385"), null));
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology10-65-1442255385"), null));

    assertEquals(true, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology1-65-1442255385"));
    assertEquals(false, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology2-65-1442255385"));
  }

  @Test
  public void testGetOfferResourcesListPerNode() {
    DefaultScheduler defaultScheduler = new DefaultScheduler();
    RotatingMap<OfferID, Offer> rotatingMap = new RotatingMap<OfferID, Offer>(2);

    String hostName = "host1.east";

    Offer offer = buildOffer("offer1", hostName, 0, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer2", hostName, 10, 0);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer3", hostName, 0, 100.01);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer4", hostName, 1.001, 0);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer5", hostName, 0, 0.001);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer6", hostName, 0.001, 0.01);
    rotatingMap.put(offer.getId(), offer);

    Map<String, List<OfferResources>> offerResourcesMap = SchedulerUtils.getOfferResourcesListPerNode(rotatingMap);
    assertEquals(offerResourcesMap.size(), 1);

    List<OfferResources> offerResources = offerResourcesMap.get("host1.east");
    assertEquals(offerResources.size(), 6);

    hostName = "host1.west";
    offer = buildOffer("offer7", hostName, 0, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer8", hostName, 10, 0);
    rotatingMap.put(offer.getId(), offer);

    offerResourcesMap = SchedulerUtils.getOfferResourcesListPerNode(rotatingMap);
    assertEquals(offerResourcesMap.size(), 2);

    offerResources = offerResourcesMap.get("host1.east");
    assertEquals(offerResources.size(), 6);

    offerResources = offerResourcesMap.get("host1.west");
    assertEquals(offerResources.size(), 2);
  }

  private TopologyDetails constructTopologyDetails(String topologyName, int numWorkers, double numCpus, double memSize) {
    Map<String, TopologyDetails> topologyConf1 = new HashMap<>();

    StormTopology stormTopology = new StormTopology();
    TopologyDetails topologyDetails= new TopologyDetails(topologyName, topologyConf1, stormTopology, numWorkers);
    topologyDetails.getConf().put(MesosCommon.WORKER_CPU_CONF, Double.valueOf(numCpus));
    topologyDetails.getConf().put(MesosCommon.WORKER_MEM_CONF, Double.valueOf(memSize));

    return topologyDetails;
  }

  private TopologyDetails constructTopologyDetails(String topologyName, int numWorkers) {
    Map<String, TopologyDetails> topologyConf1 = new HashMap<>();

    StormTopology stormTopology = new StormTopology();
    TopologyDetails topologyDetails= new TopologyDetails(topologyName, topologyConf1, stormTopology, numWorkers);

    return topologyDetails;
  }

  @Test
  public void testAllSlotsAvailableForScheduling() {
    DefaultScheduler defaultScheduler = new DefaultScheduler();

    Map<String, Object> mesosStormConf = new HashMap<>();
    defaultScheduler.prepare(mesosStormConf);


    RotatingMap<OfferID, Offer> rotatingMap = new RotatingMap<OfferID, Offer>(2);

    String hostName = "host1.east";

    Set<String> topologiesMissingAssignments = new HashSet<>();
    topologiesMissingAssignments = new HashSet<>();
    topologiesMissingAssignments.add("test-topology1-65-1442255385");
    topologiesMissingAssignments.add("test-topology1-65-1442255385");

    Collection<SupervisorDetails> existingSupervisors = new ArrayList<>();
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology1-65-1442255385"), null));
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology10-65-1442255385"), null));

    Map<String, TopologyDetails> topologyMap = new HashMap<>();
    String topologyName = "test-topology1-65-1442255385";
    topologyMap.put(topologyName, constructTopologyDetails(topologyName, 1, 0.1, 100));
    Topologies topologies = new Topologies(topologyMap);

    List<WorkerSlot> workerSlotsAvailableForScheduling;

    /* Offer with no ports but enough memory and cpu*/
    Offer offer = buildOffer("offer1", hostName, 10, 20000);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 0);


    /* Offer with no cpu but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", hostName, 0.0, 1000, 3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with no memory but enough ports and cpu */
    offer = buildOfferWithPorts("offer1", hostName, 0.0, 1000, 3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies, topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(),0);


    /* Offer with just enough ports, memory and cpu */
    /* Case 1 - Supervisor exists for topology test-topology1-65-1442255385 on the host */
    offer = buildOfferWithPorts("offer1", hostName, 0.1, 200, 3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, topologies,
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 1);

    /* Case 2 - Supervisor does not exists for topology test-topology1-65-1442255385 on the host */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 0.1, 200, 3100, 3101);
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
                                MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    TopologyDetails topologyDetails = constructTopologyDetails(topologyName, 1);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 1);


    /* More than 1 worker slot is required - Plenty of memory & cpu is available, only two ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3101);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* More than 1 worker slot is required - Plenty of ports & cpu is available, but memory is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 10 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                2 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3200);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* More than 1 worker slot is required - Plenty of ports & memory are available, but cpu is available for only two workers */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 2 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                10 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3200);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 2);

    /* 10 worker slots are required - Plenty of cpu, memory & ports are available */
    offer = buildOfferWithPorts("offer1", "host-without-supervisor.east", 20 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                20 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3200);
    rotatingMap.put(offer.getId(), offer);
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());
    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 10);

    /* 10 worker slots are available but offers are fragmented on one host */
    offer = buildOffer("offer1", hostName, 0, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer2", hostName, 10, 0);
    rotatingMap.put(offer.getId(), offer);
    hostName = "host1.west";
    offer = buildOffer("offer3", hostName, 0.01, 1000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer4", hostName, 0.1, 9000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOffer("offer5", hostName, 0.91, 9000);
    rotatingMap.put(offer.getId(), offer);
    offer = buildOfferWithPorts("offer6", hostName, 5 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                5 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3105);
    rotatingMap.put(offer.getId(), offer);

    topologyMap.clear();
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    // Increase available cpu by a tiny fraction in order
    offer = buildOfferWithPorts("offer6", hostName, 5 * MesosCommon.DEFAULT_WORKER_CPU + 1.1 * MesosCommon.DEFAULT_EXECUTOR_CPU,
                                5 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3105);
    rotatingMap.put(offer.getId(), offer);

    topologyMap.clear();
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 5); // Note that by increasing the executor cpu by a fraction, we are able to get 5 worker slots as we expect


    topologyMap.clear();
    topologyDetails = constructTopologyDetails(topologyName, 10);
    topologyMap.put(topologyName, topologyDetails);
    defaultScheduler.prepare(topologyDetails.getConf());

    workerSlotsAvailableForScheduling = defaultScheduler.allSlotsAvailableForScheduling(rotatingMap, existingSupervisors, new Topologies(topologyMap),
                                                                                        topologiesMissingAssignments);
    assertEquals(workerSlotsAvailableForScheduling.size(), 5);

    hostName = "host2.east";
    offer = buildOfferWithPorts("offer7", hostName, 3 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                3 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3200);
    rotatingMap.put(offer.getId(), offer);

    hostName = "host3.east";
    offer = buildOfferWithPorts("offer8", hostName, 100 * MesosCommon.DEFAULT_WORKER_CPU + MesosCommon.DEFAULT_EXECUTOR_CPU,
                                100 * MesosCommon.DEFAULT_WORKER_MEM_MB + MesosCommon.DEFAULT_EXECUTOR_MEM_MB, 3100, 3110);
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

    Integer[] expectedWorkerCountArray = { 3, 3, 4 };
    Integer[] actualWorkerCountArray = new Integer[3];
    actualWorkerCountArray[0] = workerCountPerHostMap.get("host1.west");
    actualWorkerCountArray[1] = workerCountPerHostMap.get("host2.east");
    actualWorkerCountArray[2] = workerCountPerHostMap.get("host3.east");

    assertArrayEquals(expectedWorkerCountArray, actualWorkerCountArray);


  }

}
