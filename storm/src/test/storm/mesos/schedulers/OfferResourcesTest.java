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

import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static storm.mesos.TestUtils.buildOffer;
import static storm.mesos.TestUtils.buildOfferWithPorts;
import static storm.mesos.TestUtils.buildOfferWithReservation;

@RunWith(MockitoJUnitRunner.class)
public class OfferResourcesTest {

  private final String sampleHost = "host1.east";

  @Test
  public void testOfferResources() throws Exception {
    Offer offer = buildOfferWithReservation("offer1", sampleHost, 2, 1000, 6, 1000);
    OfferResources offerResources = new OfferResources(offer);
    assertEquals(8, offerResources.getCpu(), 0.0f);
    assertEquals(2000, offerResources.getMem(), 0.0f);
    assertEquals(sampleHost, offerResources.getHostName());

    offer = buildOffer("offer1", sampleHost, 2.0, 2.0);
    offerResources = new OfferResources(offer);
    assertEquals(2, offerResources.getCpu(), 0.0);
    assertEquals(2, offerResources.getMem(), 0.0);
    assertEquals(sampleHost, offerResources.getHostName());

    offer = buildOfferWithPorts("offer1", sampleHost, 2.0, 2000, 3000, 3100);
    offerResources = new OfferResources(offer);
    assertEquals(2, offerResources.getCpu(), 0.0);
    assertEquals(2000, offerResources.getMem(), 0.0);
    assertEquals(true, offerResources.hasPort());

    offerResources.decCpu(1);
    offerResources.decMem(1000);
    assertEquals(1, offerResources.getCpu(), 0.0);
    assertEquals(1000, offerResources.getMem(), 0.0);
    assertEquals(3000, offerResources.getPort());
    assertEquals(3001, offerResources.getPort());
  }
}
