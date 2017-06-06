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

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ResourceType;
import storm.mesos.util.MesosCommon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// TODO(dskarthick) : Leverage the build methods defined in TestUtils function.
public class MesosNimbusTest {

  public static final Integer DEFAULT_WORKER_COUNT = 2;
  public static final Integer DEFAULT_BUCKET_COUNT = 2;
  public static final String FRAMEWORK_ROLE = "staas";

  private Map<OfferID, Offer> map = null;
  Map<String, Collection<WorkerSlot>> slotsForTopologiesNeedingAssignments = null;
  MesosNimbus mesosNimbus = null;
  Map<String, String> mesosStormConf;

  private boolean hasResources(String role, List<Protos.Resource> resourceList, Double cpus, Double mem, Long port) {
    Double actualCpu = 0.0d, actualMem = 0.0d;
    List<Long> expectedPorts = new ArrayList<>();
    expectedPorts.add(port);

    List<Long> actualPorts = new ArrayList<>();
    for (Protos.Resource resource : resourceList) {
      if (!resource.getRole().equals(role)) {
        continue;
      }
      ResourceType r = ResourceType.of(resource.getName());
      switch (r) {
        case CPU:
          actualCpu += resource.getScalar().getValue();
          break;
        case MEM:
          actualMem += resource.getScalar().getValue();
          break;
        case PORTS:
          Protos.Value.Ranges ranges = resource.getRanges();
          for (Protos.Value.Range range : ranges.getRangeList()) {
            long endValue = range.getEnd();
            long beginValue = range.getBegin();
            while (endValue >= beginValue) {
              actualPorts.add(beginValue);
              ++beginValue;
            }
          }
      }
    }

    boolean hasExpectedPorts = (expectedPorts.size() == actualPorts.size()) && expectedPorts.containsAll(actualPorts);
    return actualCpu.equals(cpus) && actualMem.equals(mem) && hasExpectedPorts;
  }

  private boolean hasResources(List<Protos.Resource> resourceList, Double cpus, Double mem, Long port) {
    Double actualCpu = 0.0d, actualMem = 0.0d;
    List<Long> expectedPorts = new ArrayList<>();
    expectedPorts.add(port);

    List<Long> actualPorts = new ArrayList<>();
    for (Protos.Resource resource : resourceList) {
      ResourceType r = ResourceType.of(resource.getName());
      switch (r) {
        case CPU:
          actualCpu += resource.getScalar().getValue();
          break;
        case MEM:
          actualMem += resource.getScalar().getValue();
          break;
        case PORTS:
          Protos.Value.Ranges ranges = resource.getRanges();
          for (Protos.Value.Range range : ranges.getRangeList()) {
            long endValue = range.getEnd();
            long beginValue = range.getBegin();
            while (endValue >= beginValue) {
              actualPorts.add(beginValue);
              ++beginValue;
            }
          }
        }
      }

    boolean hasExpectedPorts = (expectedPorts.size() == actualPorts.size()) && expectedPorts.containsAll(actualPorts);
    return actualCpu.equals(cpus) && actualMem.equals(mem) && hasExpectedPorts;
  }

  private boolean hasResources(List<Protos.Resource> resourceList, Double cpus, Double mem) {
    Double actualCpu = 0.0d, actualMem = 0.0d;

    for (Protos.Resource resource : resourceList) {
      ResourceType r = ResourceType.of(resource.getName());
      switch (r) {
        case CPU:
          actualCpu += resource.getScalar().getValue();
          break;
        case MEM:
          actualMem += resource.getScalar().getValue();
          break;
      }
    }

    return actualCpu.equals(cpus) && actualMem.equals(mem);
  }

  private boolean hasResources(String role, Protos.TaskInfo taskInfo, Double cpus, Double mem) {
    Double actualCpu = 0.0d, actualMem = 0.0d;

    for (Protos.Resource resource : taskInfo.getResourcesList()) {
      ResourceType r = ResourceType.of(resource.getName());
      if (!resource.getRole().equals(role)) {
        continue;
      }
      switch (r) {
        case CPU:
          actualCpu += resource.getScalar().getValue();
          break;
        case MEM:
          actualMem += resource.getScalar().getValue();
          break;
      }
    }

    return actualCpu.equals(cpus) && actualMem.equals(mem);
  }

  private boolean hasResources(Protos.TaskInfo taskInfo, Double cpus, Double mem, Long port) {
    return hasResources(taskInfo.getResourcesList(), cpus, mem, port);
  }

  private boolean hasResources(Protos.TaskInfo taskInfo, Double cpus, Double mem) {
    return hasResources(taskInfo.getResourcesList(), cpus, mem);
  }

  private boolean hasResources(String role, Protos.TaskInfo taskInfo, Double cpus, Double mem, Long port) {
    return hasResources(role, taskInfo.getResourcesList(), cpus, mem, port);
  }

  private boolean hasCorrectExecutorResources(List<Protos.TaskInfo> taskInfoList) {
    for (Protos.TaskInfo taskInfo : taskInfoList) {
      if (!hasResources(taskInfo.getExecutor().getResourcesList(), MesosCommon.DEFAULT_EXECUTOR_CPU, MesosCommon.DEFAULT_EXECUTOR_MEM_MB)) {
        return false;
      }
    }
    return true;
  }

  private String getTopologyIdFromTaskName(String taskName) {
    String info[] = taskName.split("\\|");
    return info[1];
  }

  private Map<String, List<Protos.TaskInfo>> getTopologyIDtoTaskInfoMap(List<Protos.TaskInfo> taskInfoList) {
    Map<String, List<Protos.TaskInfo>> topologyIDtoTaskInfoMap = new HashMap<>();

    for (Protos.TaskInfo taskInfo: taskInfoList) {
      String topologyId = getTopologyIdFromTaskName(taskInfo.getName()).trim();
      if (!topologyIDtoTaskInfoMap.containsKey(topologyId)) {
        topologyIDtoTaskInfoMap.put(topologyId, new ArrayList<Protos.TaskInfo>());
      }
      topologyIDtoTaskInfoMap.get(topologyId).add(taskInfo);
    }
    return topologyIDtoTaskInfoMap;
  }

  @Before
  public void initialize() {
    map = new HashMap<OfferID, Offer>();
    slotsForTopologiesNeedingAssignments = new HashMap<>();

    mesosStormConf = new HashMap<>();
    mesosStormConf.put(MesosNimbus.CONF_EXECUTOR_URI, "/fake/path/to/storm-mesos.tgz");
    mesosStormConf.put(MesosCommon.CONF_MESOS_ROLE, FRAMEWORK_ROLE);
    mesosNimbus = Mockito.spy(new MesosNimbus());

    mesosNimbus.initializeMesosStormConf(mesosStormConf, "/mock");
    Mockito.doReturn("http://127.0.0.1/").when(mesosNimbus).getFullConfigUri();
  }

  @Test
  public void testGetTasksToLaunchWhenNoTopologiesNeedAssignments() {
    TopologyDetails t1 = TestUtils.constructTopologyDetails("t1",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);
    Map<String, TopologyDetails> topologyDetailsMap = new HashMap<>();
    topologyDetailsMap.put("t1", t1);

    Topologies topologies = new Topologies(topologyDetailsMap);

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    Map<String, Collection<WorkerSlot>> workerSlotsMap = new HashMap<>();

    Map<String,List<Protos.TaskInfo>> tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);

    assertTrue(tasksToLaunch.isEmpty());
  }

  @Test
  public void testGetTasksToLaunchForOneTopologyWithOneOffer() {
    TopologyDetails t1 = TestUtils.constructTopologyDetails("t1",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);
    Map m = t1.getConf();
    Map<String, TopologyDetails> topologyDetailsMap = new HashMap<>();
    topologyDetailsMap.put("t1", t1);

    Topologies topologies = new Topologies(topologyDetailsMap);

    // One offer with sufficient resources
    Offer offer = TestUtils.buildOfferWithPorts("O-1", "h1", 24, 40000, 3100, 3200);
    map.put(offer.getId(), offer);

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    Map<String, Collection<WorkerSlot>> workerSlotsMap = new HashMap<>();
    Collection<WorkerSlot> workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3100));
    workerSlotsMap.put("t1", workerSlots);

    Map<String,List<Protos.TaskInfo>> tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);

    assertTrue((tasksToLaunch.size() == 1));
    assertTrue((tasksToLaunch.get("h1").size() == 1));

    // One offer with sufficient resources spread across reserved and unreserved resources
    offer = TestUtils.buildOfferWithReservationAndPorts("O-1", "h1", 0.75, 750, 0.75, 850, 3100, 3101);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    workerSlotsMap = new HashMap<>();
    workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3100));
    workerSlotsMap.put("t1", workerSlots);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);

    assertTrue((tasksToLaunch.size() == 1));
    assertTrue((tasksToLaunch.get("h1").size() == 1));
    assertTrue(hasResources(FRAMEWORK_ROLE, tasksToLaunch.get("h1").get(0), 0.75 - MesosCommon.DEFAULT_EXECUTOR_CPU, 850 - MesosCommon.DEFAULT_EXECUTOR_MEM_MB));
    assertTrue(hasResources("*", tasksToLaunch.get("h1").get(0), 0.35, 650.0));
    assertTrue(hasCorrectExecutorResources(tasksToLaunch.get("h1")));
    assertEquals(0.4f, TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.CPU), 0.01f);
    assertEquals(100f, TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.MEM), 0.01f);

    // One offer with only reserved resources
    offer = TestUtils.buildOfferWithReservationAndPorts("O-1", "h1", 0, 0, 1.5, 1600, 3100, 3101);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    workerSlotsMap = new HashMap<>();
    workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3100));
    workerSlotsMap.put("t1", workerSlots);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);

    assertTrue((tasksToLaunch.size() == 1));
    assertTrue(tasksToLaunch.get("h1").size() == 1);
    assertTrue(hasResources(FRAMEWORK_ROLE, tasksToLaunch.get("h1").get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB));
    assertTrue(hasCorrectExecutorResources(tasksToLaunch.get("h1")));
    assertEquals(TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.CPU), 0.4f, 0.01f);
    assertEquals(TestUtils.calculateAllAvailableScalarResources(aggregatedOffersPerNode.get("h1"), ResourceType.MEM), 100f, 0.01f);

    // Offer with Insufficient cpu
    offer = TestUtils.buildOfferWithPorts("O-1", "h1", 0, 40000, 3100, 3200);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    assertTrue(tasksToLaunch.isEmpty());

    // Offer with Insufficient Mem for both executor and worker combined
    offer = TestUtils.buildOfferWithPorts("O-1", "h1", 24, 900, 3100, 3200);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    assertTrue(tasksToLaunch.isEmpty());

    // Offer with Insufficient Mem for executor
    offer = TestUtils.buildOfferWithPorts("O-1", "h1", 24, 1400, 3100, 3200);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    assertTrue(tasksToLaunch.isEmpty());

    // One offer with Insufficient ports
    offer = TestUtils.buildOffer("O-1", "h1", 24, 4000);
    map.put(offer.getId(), offer);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    assertTrue(tasksToLaunch.isEmpty());
  }

  @Test
  public void testGetTasksToLaunchForOneTopologyWithMultipleOffersOnSameHost() {
    TopologyDetails t1 = TestUtils.constructTopologyDetails("t1",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);
    Map<String, TopologyDetails> topologyDetailsMap = new HashMap<>();
    topologyDetailsMap.put("t1", t1);

    Topologies topologies = new Topologies(topologyDetailsMap);

    Offer offer = TestUtils.buildOffer("O-1", "h1", 0, 40000);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOffer("O-2", "h1", 24, 0);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOfferWithPorts("O-3", "h1", 0, 0, 3100, 3200);
    map.put(offer.getId(), offer);

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    Map<String, Collection<WorkerSlot>> workerSlotsMap = new HashMap<>();
    Collection<WorkerSlot> workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3100));
    workerSlotsMap.put("t1", workerSlots);
    Map<String,List<Protos.TaskInfo>> tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    assertTrue((tasksToLaunch.size() == 1));
    assertTrue((tasksToLaunch.get("h1").size() == 1));
    List<Protos.TaskInfo> taskInfoList = tasksToLaunch.get("h1");
    assertTrue(hasResources("*", taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3100l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    workerSlots.add(new WorkerSlot("h1", 3101));
    workerSlots.add(new WorkerSlot("h1", 3102));
    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    taskInfoList = tasksToLaunch.get("h1");
    assertTrue(taskInfoList.size() == 3);
    assertTrue(hasResources("*", taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3100l));
    assertTrue(hasResources("*", taskInfoList.get(1), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3101l));
    assertTrue(hasResources("*", taskInfoList.get(2), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3102l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));

    TopologyDetails t2 = TestUtils.constructTopologyDetails("t2",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);
    workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3103));
    workerSlots.add(new WorkerSlot("h1", 3104));
    workerSlotsMap.put("t2", workerSlots);

    topologyDetailsMap = new HashMap<>();
    topologyDetailsMap.put("t1", t1);
    topologyDetailsMap.put("t2", t2);
    topologies = new Topologies(topologyDetailsMap);

    aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);
    Map<String, List<Protos.TaskInfo>> topologyIDtoTaskInfoMap = getTopologyIDtoTaskInfoMap(tasksToLaunch.get("h1"));
    taskInfoList = topologyIDtoTaskInfoMap.get("t1");
    assertTrue(hasResources("*", taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3100l));
    assertTrue(hasResources("*", taskInfoList.get(1), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3101l));
    assertTrue(hasResources("*", taskInfoList.get(2), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3102l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));

    taskInfoList = topologyIDtoTaskInfoMap.get("t2");
    assertTrue(hasResources("*", taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3103l));
    assertTrue(hasResources("*", taskInfoList.get(1), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3104l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));
  }

  @Test
  public void testGetTasksToLaunchForOneTopologyWithMultipleOffersAcrossMultipleHosts() {
    TopologyDetails t1 = TestUtils.constructTopologyDetails("t1",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);
    TopologyDetails t2 = TestUtils.constructTopologyDetails("t2",
                                                            MesosNimbusTest.DEFAULT_WORKER_COUNT,
                                                            MesosCommon.DEFAULT_WORKER_CPU,
                                                            MesosCommon.DEFAULT_WORKER_MEM_MB);

    Map<String, TopologyDetails> topologyDetailsMap = new HashMap<>();
    topologyDetailsMap.put("t1", t1);
    topologyDetailsMap.put("t2", t2);

    Topologies topologies = new Topologies(topologyDetailsMap);

    Offer offer = TestUtils.buildOffer("O-H1-1", "h1", 0, 4000);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOffer("O-H1-2", "h1", 3.21, 0);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOfferWithPorts("O-H1-3", "h1", 0, 0, 3100, 3102);
    map.put(offer.getId(), offer);

    offer = TestUtils.buildOffer("O-H2-1", "h2", 0, 4000);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOffer("O-H2-2", "h2", 3.21, 0);
    map.put(offer.getId(), offer);
    offer = TestUtils.buildOfferWithPorts("O-H2-3", "h2", 0, 0, 3100, 3102);
    map.put(offer.getId(), offer);

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(map);
    Map<String, Collection<WorkerSlot>> workerSlotsMap = new HashMap<>();
    Map<String, List<Protos.TaskInfo>> tasksToLaunch = new HashMap<>();

    List<WorkerSlot> workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h1", 3100));
    workerSlots.add(new WorkerSlot("h2", 3101));
    workerSlots.add(new WorkerSlot("h1", 3102));
    workerSlotsMap.put("t1", workerSlots);

    workerSlots = new ArrayList<>();
    workerSlots.add(new WorkerSlot("h2", 3100));
    workerSlots.add(new WorkerSlot("h1", 3101));
    workerSlots.add(new WorkerSlot("h2", 3102));
    workerSlotsMap.put("t2", workerSlots);

    tasksToLaunch = mesosNimbus.getTasksToLaunch(topologies, workerSlotsMap, aggregatedOffersPerNode);

    List<Protos.TaskInfo> taskInfoList = new ArrayList<>();
    for(List<Protos.TaskInfo> til : tasksToLaunch.values()) {
      taskInfoList.addAll(til);
    }

    Map<String, List<Protos.TaskInfo>> topologyIDtoTaskInfoMap = getTopologyIDtoTaskInfoMap(taskInfoList);
    taskInfoList = topologyIDtoTaskInfoMap.get("t1");
    assertTrue(hasResources(taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3100l));
    assertTrue(hasResources(taskInfoList.get(1), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3102l));
    assertTrue(hasResources(taskInfoList.get(2), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3101l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));
    taskInfoList = topologyIDtoTaskInfoMap.get("t2");
    assertTrue(hasResources(taskInfoList.get(0), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3101l));
    assertTrue(hasResources(taskInfoList.get(1), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3100l));
    assertTrue(hasResources(taskInfoList.get(2), MesosCommon.DEFAULT_WORKER_CPU, MesosCommon.DEFAULT_WORKER_MEM_MB, 3102l));
    assertTrue(hasCorrectExecutorResources(taskInfoList));
  }
}
