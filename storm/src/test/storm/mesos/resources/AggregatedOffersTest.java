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
package storm.mesos.resources;

import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import storm.mesos.TestUtils;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class AggregatedOffersTest {
  private static final double DELTA_FOR_DOUBLE_COMPARISON = 0.0001;

  @Test
  public void testToIgnoreDynamicResources() {
    ScalarResource scalarResource = new ScalarResource(ResourceType.CPU);
    scalarResource.add(new ResourceEntries.ScalarResourceEntry(100.0), ReservationType.STATIC);
    scalarResource.toString();

    // Note that buidOffer adds
    Offer offer = TestUtils.buildOffer("0-1", "h1", 0, 0);
    AggregatedOffers aggregatedOffers = new AggregatedOffers(offer);

    assertEquals(0, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU), DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(0, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM), DELTA_FOR_DOUBLE_COMPARISON);

    assertTrue(aggregatedOffers.getHostName().equals(offer.getHostname()));
    assertTrue(aggregatedOffers.getSlaveID().equals(offer.getSlaveId()));

    offer = TestUtils.buildOfferWithReservation("offer1", "h1", 2, 1000, 6, 1000);
    aggregatedOffers = new AggregatedOffers(offer);
    assertEquals(8, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU), DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(2000, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM), DELTA_FOR_DOUBLE_COMPARISON);
    assertTrue(aggregatedOffers.getHostName().equals(offer.getHostname()));
    assertTrue(aggregatedOffers.getSlaveID().equals(offer.getSlaveId()));

    offer = TestUtils.buildOfferWithPorts("offer1", "h1", 2.0, 2000, 3000, 3100);
    aggregatedOffers = new AggregatedOffers(offer);
    assertEquals(2.0, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.CPU), DELTA_FOR_DOUBLE_COMPARISON);
    assertEquals(2000, TestUtils.calculateAllAvailableScalarResources(aggregatedOffers, ResourceType.MEM),DELTA_FOR_DOUBLE_COMPARISON);
    List<Long> rangeResources = TestUtils.calculateAllAvailableRangeResources(aggregatedOffers, ResourceType.PORTS);
    assertTrue(rangeResources.size() == 101);
  }
}
