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

import backtype.storm.scheduler.SupervisorDetails;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static storm.mesos.TestUtils.buildOffer;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerUtilsTest {

  RotatingMap<OfferID, Offer> rotatingMap;
  private final String sampleHost = "host1.east";

  @Before
  public void initialize() {
    rotatingMap = new RotatingMap<>(2);
  }

  private void buildOfferAndUpdateRotatingMap(String offerId, String hostName, double cpus, double memory) {
    Offer offer = buildOffer(offerId, hostName, cpus, memory);
    rotatingMap.put(offer.getId(), offer);
  }

  @Test
  public void testSupervisorExists() throws Exception {
    Collection<SupervisorDetails> existingSupervisors = new ArrayList<>();
    String hostName = "host1.east";

    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology1-65-1442255385"), hostName, null));
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology10-65-1442255385"), hostName, null));

    assertEquals(true, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology1-65-1442255385"));
    assertEquals(false, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology2-65-1442255385"));
  }

  @Test
  public void testGetOfferResourcesListPerNode() {
    String hostName = sampleHost;

    buildOfferAndUpdateRotatingMap("offer1", hostName, 0, 1000);
    buildOfferAndUpdateRotatingMap("offer2", hostName, 10, 0);
    buildOfferAndUpdateRotatingMap("offer3", hostName, 0, 100.01);
    buildOfferAndUpdateRotatingMap("offer4", hostName, 1.001, 0);
    buildOfferAndUpdateRotatingMap("offer5", hostName, 0, 0.001);
    buildOfferAndUpdateRotatingMap("offer6", hostName, 0.001, 0.01);

    Map<String, List<OfferResources>> offerResourcesMap = SchedulerUtils.getOfferResourcesListPerNode(rotatingMap);
    assertEquals(offerResourcesMap.size(), 1);

    List<OfferResources> offerResources = offerResourcesMap.get("host1.east");
    assertEquals(offerResources.size(), 6);

    hostName = "host1.west";
    buildOfferAndUpdateRotatingMap("offer7", hostName, 0, 1000);
    buildOfferAndUpdateRotatingMap("offer8", hostName, 10, 0);

    offerResourcesMap = SchedulerUtils.getOfferResourcesListPerNode(rotatingMap);
    assertEquals(offerResourcesMap.size(), 2);

    offerResources = offerResourcesMap.get("host1.east");
    assertEquals(offerResources.size(), 6);

    offerResources = offerResourcesMap.get("host1.west");
    assertEquals(offerResources.size(), 2);
  }
}
