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

import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import storm.mesos.util.MesosCommon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static storm.mesos.TestUtils.buildOffer;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerUtilsTest {

  Map<OfferID, Offer> map;
  private final String sampleHost = "host1.east";

  @Before
  public void initialize() {
    map = new HashMap<OfferID, Offer>();
  }

  private void buildOfferAndUpdateMap(String offerId, String hostName, double cpus, double memory) {
    Offer offer = buildOffer(offerId, hostName, cpus, memory);
    map.put(offer.getId(), offer);
  }

  @Test
  public void testSupervisorExists() throws Exception {
    Collection<SupervisorDetails> existingSupervisors = new ArrayList<>();
    String hostName = "host1.east";

    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology1-65-1442255385"), hostName));
    existingSupervisors.add(new SupervisorDetails(MesosCommon.supervisorId(hostName, "test-topology10-65-1442255385"), hostName));

    assertEquals(true, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology1-65-1442255385"));
    assertEquals(false, SchedulerUtils.supervisorExists(hostName, existingSupervisors, "test-topology2-65-1442255385"));
  }
}
