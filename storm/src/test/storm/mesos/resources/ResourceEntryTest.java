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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertTrue;
import static storm.mesos.resources.ResourceEntries.RangeResourceEntry;

@RunWith(MockitoJUnitRunner.class)
public class ResourceEntryTest {
  RangeResourceEntry existingRange;

  private boolean isEqual(RangeResourceEntry r1, RangeResourceEntry r2) {
    return r1.getBegin().equals(r2.getBegin()) && r1.getEnd().equals(r2.getEnd());
  }

  @Test
  public void testDisjointAndNonAdjacentRanges() {
    Long beginPort = 1000l;
    Long endPort = 2000l;

    existingRange = new RangeResourceEntry(beginPort, endPort);
    RangeResourceEntry originalRange = new RangeResourceEntry(beginPort, endPort);

    RangeResourceEntry newRange = new RangeResourceEntry(beginPort - 1000, beginPort - 2);
    existingRange.add(newRange);
    assertTrue(isEqual(originalRange, existingRange));

    newRange = new RangeResourceEntry(endPort + 2, endPort + 1000);
    existingRange.add(newRange);
    assertTrue(isEqual(originalRange, existingRange));
  }

  @Test
  public void testDisjointAndAdjacentRanges() {
    Long beginPort = 1000l;
    Long endPort = 2000l;

    existingRange = new RangeResourceEntry(beginPort, endPort);

    RangeResourceEntry newRange = new RangeResourceEntry(endPort + 1, endPort + 1000);
    RangeResourceEntry expectedRange = new RangeResourceEntry(beginPort, endPort + 1000);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));

    existingRange = new RangeResourceEntry(beginPort, endPort);
    newRange = new RangeResourceEntry(beginPort - 1000, beginPort - 1);
    expectedRange = new RangeResourceEntry(beginPort - 1000, endPort);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));
  }

  @Test
  public void testOverlappingRanges() {
    Long beginPort = 1000l;
    Long endPort = 2000l;
    RangeResourceEntry newRange;
    RangeResourceEntry expectedRange;

    existingRange = new RangeResourceEntry(beginPort, endPort);

    existingRange = new RangeResourceEntry(beginPort, endPort);
    newRange = new RangeResourceEntry(beginPort - 500, beginPort + 500);
    expectedRange = new RangeResourceEntry(beginPort - 500, endPort);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));

    existingRange = new RangeResourceEntry(beginPort, endPort);
    newRange = new RangeResourceEntry(beginPort + 500, endPort + 500);
    expectedRange = new RangeResourceEntry(beginPort, endPort + 500);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));
  }

  @Test
  public void testSubSets() {
    Long beginPort = 1000l;
    Long endPort = 2000l;
    RangeResourceEntry newRange;
    RangeResourceEntry expectedRange;

    existingRange = new RangeResourceEntry(beginPort, endPort);

    newRange = new RangeResourceEntry(beginPort + 250, beginPort + 500);
    expectedRange = new RangeResourceEntry(beginPort, endPort);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));
  }

  @Test
  public void testSuperSets() {
    Long beginPort = 1000l;
    Long endPort = 2000l;
    RangeResourceEntry newRange;
    RangeResourceEntry expectedRange;

    existingRange = new RangeResourceEntry(beginPort, endPort);

    newRange = new RangeResourceEntry(beginPort - 250, endPort + 250);
    expectedRange = new RangeResourceEntry(beginPort - 250, endPort + 250);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));
  }

  @Test
  public void testInvalidRange() {
    Long beginPort = 1000l;
    Long endPort = 2000l;
    RangeResourceEntry newRange;
    RangeResourceEntry expectedRange;

    existingRange = new RangeResourceEntry(beginPort, endPort);

    newRange = new RangeResourceEntry(endPort, beginPort);
    expectedRange = new RangeResourceEntry(beginPort, endPort);
    existingRange.add(newRange);
    assertTrue(isEqual(expectedRange, existingRange));
  }
}
