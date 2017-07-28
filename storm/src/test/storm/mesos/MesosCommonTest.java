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

import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.mesos.Protos;
import org.junit.Before;
import org.junit.Test;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ReservationType;
import storm.mesos.resources.ResourceType;
import storm.mesos.util.MesosCommon;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosCommonTest {

  private Map conf;
  private TopologyDetails info;
  private String topologyName = "t_name";
  private static final double DELTA_FOR_DOUBLE_COMPARISON = 0.0001;
  private final MesosNimbus mesosNimbus;

  public MesosCommonTest() {
    Map mesosStormConfig = new HashMap<>();
    mesosNimbus = new MesosNimbus();
    mesosNimbus.initializeMesosStormConf(mesosStormConfig, "/mock");
  }

  public void initConf() {
    conf = new HashMap<>();
    conf.put("topology.name", topologyName);
    TestUtils.initializeStormTopologyConfig(conf);
  }

  @Before
  public void initTest() {
    initConf();
    info = new TopologyDetails("t1", conf, new StormTopology(), 1);
  }

  @Test
  public void testHostFromAssignmentId() throws Exception {
    String assignmentId = "a-test-assignment-id-host";
    String delimiter = "-";

    String result = MesosCommon.hostFromAssignmentId(assignmentId, delimiter);
    String expectedResult = "host";

    assertEquals(result, expectedResult);
  }

  @Test
  public void testGetWorkerPrefix() throws Exception {

    // Test default values
    String result = MesosCommon.getWorkerPrefix(conf, info);
    String expectedResult = topologyName + MesosCommon.DEFAULT_WORKER_NAME_PREFIX_DELIMITER;
    assertEquals(result, expectedResult);

    // Test explicit value of prefix
    String prefix = ":(";
    conf.put(MesosCommon.WORKER_NAME_PREFIX, prefix);
    info = new TopologyDetails("t2", conf, new StormTopology(), 1);
    result = MesosCommon.getWorkerPrefix(conf, info);
    expectedResult = prefix + topologyName + MesosCommon.DEFAULT_WORKER_NAME_PREFIX_DELIMITER;
    assertEquals(result, expectedResult);
    conf.remove(MesosCommon.WORKER_NAME_PREFIX);

    // Test explicit value of delimiter
    String delimiter = ":)";
    conf.put(MesosCommon.WORKER_NAME_PREFIX_DELIMITER, delimiter);
    info = new TopologyDetails("t3", conf, new StormTopology(), 1);
    result = MesosCommon.getWorkerPrefix(conf, info);
    expectedResult = topologyName + delimiter;
    assertEquals(result, expectedResult);

    // Test explicit value of prefix and delimiter
    conf.put(MesosCommon.WORKER_NAME_PREFIX, prefix);
    info = new TopologyDetails("t4", conf, new StormTopology(), 2);
    result = MesosCommon.getWorkerPrefix(conf, info);
    expectedResult = prefix + topologyName + delimiter;
    assertEquals(result, expectedResult);
  }

  @Test
  public void testGetMesosComponentNameDelimiter() throws Exception {
    String topologyName = "t_name";

    // Test default values
    String result = MesosCommon.getMesosComponentNameDelimiter(conf, info);
    String expectedResult = MesosCommon.DEFAULT_MESOS_COMPONENT_NAME_DELIMITER;
    assertEquals(result, expectedResult);

    // Test explicit value
    info = new TopologyDetails("t2", conf, new StormTopology(), 2);
    String delimiter = "-";
    conf.put(MesosCommon.MESOS_COMPONENT_NAME_DELIMITER, delimiter);
    result = MesosCommon.getMesosComponentNameDelimiter(conf, info);
    expectedResult = delimiter;
    assertEquals(result, expectedResult);

    // Test explicit value
    info = new TopologyDetails("t2", conf, new StormTopology(), 2);
    initConf();
    result = MesosCommon.getMesosComponentNameDelimiter(conf, info);
    expectedResult = delimiter;
    assertEquals(result, expectedResult);

    // Test explicit value overridden in topology config
    initConf();
    conf.put(MesosCommon.MESOS_COMPONENT_NAME_DELIMITER, delimiter);
    info = new TopologyDetails("t3", conf, new StormTopology(), 3);
    Map nimbusConf = new HashMap<>();
    nimbusConf.put(MesosCommon.MESOS_COMPONENT_NAME_DELIMITER, "--");
    result = MesosCommon.getMesosComponentNameDelimiter(nimbusConf, info);
    expectedResult = delimiter;
    assertEquals(result, expectedResult);
  }

  @Test
  public void testTaskId() throws Exception {
    String nodeid = "nodeID1";
    int port = 52;
    // Regex format for timestamp is %d.%03d
    String result = MesosCommon.taskId(nodeid, port);
    String[] expectedElements = result.split("-");
    // The taskId should be in at least three parts
    assertTrue(3 >= expectedElements.length);
    assertEquals(nodeid, expectedElements[0]);
    String portAsString = "" + port + "";
    assertEquals(portAsString, expectedElements[1]);
    // The last part is timestamp number, ignore for now, will be different every time
  }

  @Test
  public void testSupervisorId() throws Exception {
    String nodeid = "nodeID1";
    String topologyid = "t1";
    String result = MesosCommon.supervisorId(nodeid, topologyid);
    String expectedResult = nodeid + "|" + topologyid;
    assertEquals(result, expectedResult);
  }

  @Test
  public void testStartLogViewer() throws Exception {
    // Test the default (true)
    boolean result = MesosCommon.autoStartLogViewer(conf);
    assertTrue(result);
    conf.put(MesosCommon.AUTO_START_LOGVIEWER_CONF, false);
    result = MesosCommon.autoStartLogViewer(conf);
    assertTrue(!result);
  }

  @Test
  public void testPortFromTaskId() throws Exception {
    String taskid = "nodeid-22-time.stamp";
    int result = MesosCommon.portFromTaskId(taskid);
    int expectedResult = 22;

    assertEquals(result, expectedResult);
  }

  @Test
  public void testGetSuicideTimeout() throws Exception {
    // Test default value
    int result = MesosCommon.getSuicideTimeout(conf);
    int expectedResult = MesosCommon.DEFAULT_SUICIDE_TIMEOUT_SECS;
    assertEquals(result, expectedResult);

    // Test explicit value
    conf.put(MesosCommon.SUICIDE_CONF, 12);
    result = MesosCommon.getSuicideTimeout(conf);
    expectedResult = 12;
    assertEquals(result, expectedResult);
  }

  @Test
  public void testGetFullTopologyConfig() throws Exception {
    Map nimbusConf = new HashMap<>();
    nimbusConf.put("TEST_NIMBUS_CONFIG", 1);
    nimbusConf.put("TEST_TOPOLOGY_OVERRIDE", 2);
    Map topologyConf = new HashMap<>();
    TestUtils.initializeStormTopologyConfig(topologyConf);
    topologyConf.put("TEST_TOPOLIGY_CONFIG", 3);
    topologyConf.put("TEST_TOPOLOGY_OVERRIDE", 4);
    TopologyDetails info = new TopologyDetails("t1", topologyConf, new StormTopology(), 2);
    Map result = MesosCommon.getFullTopologyConfig(nimbusConf, info);
    Map expectedResult = new HashMap<>();
    TestUtils.initializeStormTopologyConfig(expectedResult);
    expectedResult.put("TEST_NIMBUS_CONFIG", 1);
    expectedResult.put("TEST_TOPOLIGY_CONFIG", 3);
    expectedResult.put("TEST_TOPOLOGY_OVERRIDE", 4);

    assertEquals(result, expectedResult);
  }

  @Test
  public void testTopologyWorkerCpu() throws Exception {
    // Test default value
    double result = MesosCommon.topologyWorkerCpu(conf, info);
    double expectedResult = MesosCommon.DEFAULT_WORKER_CPU;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test what happens when config is too small
    double cpuConfig = MesosCommon.MESOS_MIN_CPU;
    cpuConfig -= 0.001;
    conf.put(MesosCommon.WORKER_CPU_CONF, cpuConfig);
    result = MesosCommon.topologyWorkerCpu(conf, info);
    expectedResult = MesosCommon.DEFAULT_WORKER_CPU;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test what happens when config is null
    conf.put(MesosCommon.WORKER_CPU_CONF, null);
    result = MesosCommon.topologyWorkerCpu(conf, info);
    expectedResult = MesosCommon.DEFAULT_WORKER_CPU;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test explicit value
    conf.put(MesosCommon.WORKER_CPU_CONF, 1.5);
    info = new TopologyDetails("t2", conf, new StormTopology(), 2);
    result = MesosCommon.topologyWorkerCpu(conf, info);
    expectedResult = 1.5;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test string passed in
    initConf();
    conf.put(MesosCommon.WORKER_CPU_CONF, "2");
    info = new TopologyDetails("t3", conf, new StormTopology(), 1);
    result = MesosCommon.topologyWorkerCpu(conf, info);
    expectedResult = 1;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test that this value is not overwritten by Topology config
    Map nimbusConf = new HashMap<>();
    nimbusConf.put(MesosCommon.WORKER_CPU_CONF, 2);
    initConf();
    conf.put(MesosCommon.WORKER_CPU_CONF, 1.5);
    info = new TopologyDetails("t4", conf, new StormTopology(), 1);
    result = MesosCommon.topologyWorkerCpu(nimbusConf, info);
    expectedResult = 1.5;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);
  }

  @Test
  public void testTopologyWorkerMem() throws Exception {
    // Test default value
    double result = MesosCommon.topologyWorkerMem(conf, info);
    double expectedResult = MesosCommon.DEFAULT_WORKER_MEM_MB;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test what happens when config is too small
    double memConfig = MesosCommon.MESOS_MIN_MEM_MB;
    memConfig -= 1.0;
    conf.put(MesosCommon.WORKER_MEM_CONF, memConfig);
    result = MesosCommon.topologyWorkerMem(conf, info);
    expectedResult = MesosCommon.DEFAULT_WORKER_MEM_MB;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test what happens when config is null
    conf.put(MesosCommon.WORKER_MEM_CONF, null);
    result = MesosCommon.topologyWorkerMem(conf, info);
    expectedResult = MesosCommon.DEFAULT_WORKER_MEM_MB;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test explicit value
    conf.put(MesosCommon.WORKER_MEM_CONF, 1200);
    info = new TopologyDetails("t2", conf, new StormTopology(), 2);
    result = MesosCommon.topologyWorkerMem(conf, info);
    expectedResult = 1200;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test string passed in
    conf.put(MesosCommon.WORKER_MEM_CONF, "1200");
    info = new TopologyDetails("t3", conf, new StormTopology(), 1);
    result = MesosCommon.topologyWorkerMem(conf, info);
    expectedResult = 1000;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test that this value is overwritten by Topology config
    Map nimbusConf = new HashMap<>();
    nimbusConf.put(MesosCommon.WORKER_MEM_CONF, 200);
    conf.put(MesosCommon.WORKER_MEM_CONF, 150);
    info = new TopologyDetails("t4", conf, new StormTopology(), 1);
    result = MesosCommon.topologyWorkerMem(nimbusConf, info);
    expectedResult = 150;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);
  }

  @Test
  public void testExecutorCpu() throws Exception {
    // Test default config
    double result = MesosCommon.executorCpu(conf);
    double expectedResult = MesosCommon.DEFAULT_EXECUTOR_CPU;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test explicit value
    conf.put(MesosCommon.EXECUTOR_CPU_CONF, 2.0);
    result = MesosCommon.executorCpu(conf);
    expectedResult = 2.0;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);
  }

  @Test
  public void testExecutorMem() throws Exception {
    // Test default config
    double result = MesosCommon.executorMem(conf);
    double expectedResult = MesosCommon.DEFAULT_EXECUTOR_MEM_MB;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);

    // Test explicit value
    conf.put(MesosCommon.EXECUTOR_MEM_CONF, 100);
    result = MesosCommon.executorMem(conf);
    expectedResult = 100;
    assertEquals(result, expectedResult, DELTA_FOR_DOUBLE_COMPARISON);
  }

  @Test
  public void aggregatedOffersPerNode() {
    Map<Protos.OfferID, Protos.Offer> r = new HashMap<Protos.OfferID, Protos.Offer>();
    Protos.Offer offer = TestUtils.buildOffer("0-1", "h1", 0, 0);
    r.put(offer.getId(), offer);
    offer = TestUtils.buildOffer("0-2", "h1", 10, 1000);
    r.put(offer.getId(), offer);
    offer = TestUtils.buildOfferWithReservation("0-1", "h1", 0, 0, 200, 2000);
    r.put(offer.getId(), offer);

    offer = TestUtils.buildOffer("0-3", "h2", 0, 0);
    r.put(offer.getId(), offer);
    offer = TestUtils.buildOfferWithPorts("O-4", "h2", 0, 0, 1, 100);
    r.put(offer.getId(), offer);

    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(r);
    AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get("h1");
    assertEquals(210, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(3000, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(200, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU, ReservationType.STATIC), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(2000, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM, ReservationType.STATIC), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(0, TestUtils.calculateAllAvailableRangeResources(aggregatedOffers, ResourceType.PORTS, ReservationType.STATIC).size(), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    aggregatedOffers = aggregatedOffersPerNode.get("h2");
    assertEquals(0, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU, ReservationType.STATIC), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(0, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM, ReservationType.STATIC), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(100, TestUtils.calculateAllAvailableRangeResources(aggregatedOffers, ResourceType.PORTS, ReservationType.UNRESERVED).size(), MesosCommonTest.DELTA_FOR_DOUBLE_COMPARISON);
  }
}
