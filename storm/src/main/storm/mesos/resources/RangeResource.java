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

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static storm.mesos.resources.ResourceEntries.RangeResourceEntry;

public final class RangeResource implements Resource<RangeResourceEntry> {

  public final ResourceType resourceType;
  private final Map<ReservationType, List<RangeResourceEntry>> availableResourcesByReservationType;

  // XXX(eweathers): this is *so* close to the implementation in ScalarResource constructor.  I feel like there should be a better way of handling this.
  public RangeResource(ResourceType resourceType) {
    this.resourceType = resourceType;
    availableResourcesByReservationType = new TreeMap<>(new DefaultReservationTypeComparator());
    for (ReservationType reservationType : ReservationType.values()) {
      availableResourcesByReservationType.put(reservationType, new ArrayList<RangeResourceEntry>());
    }
  }

  @Override
  public boolean isAvailable(RangeResourceEntry rangeResourceEntry) {
    Long beginValue = rangeResourceEntry.getBegin();
    Long endValue = rangeResourceEntry.getEnd();

    for (List<RangeResourceEntry> rangeList : availableResourcesByReservationType.values()) {
      for (RangeResourceEntry r : rangeList) {
        if (beginValue >= r.getBegin() && endValue <= r.getEnd())
          return true;
      }
    }
    return false;
  }

  @Override
  public boolean isAvailable(RangeResourceEntry rangeResourceEntry, ReservationType reservationType) {
    Long beginValue = rangeResourceEntry.getBegin();
    Long endValue = rangeResourceEntry.getEnd();

    for (RangeResourceEntry r : availableResourcesByReservationType.get(reservationType)) {
      if (beginValue >= r.getBegin() && endValue <= r.getEnd())
        return true;
    }
    return false;
  }

  @Override
  public List<RangeResourceEntry> getAllAvailableResources() {
    List<RangeResourceEntry> retVal = new ArrayList<>();
    for (ReservationType reservationType : ReservationType.values()) {
      retVal.addAll(getAllAvailableResources(reservationType));
    }
    return retVal;
  }

  @Override
  public List<RangeResourceEntry> getAllAvailableResources(final ReservationType reservationType) {
    return availableResourcesByReservationType.get(reservationType);
  }

  @Override
  public void add(RangeResourceEntry rangeResourceEntry, ReservationType reservationType) {
    availableResourcesByReservationType.get(reservationType).add(rangeResourceEntry);
  }

  /**
   * Remove/Reserve range from available ranges. When resources of several reservation types ({@link storm.mesos.resources.ReservationType})
   * are available, the priority of reservation type removed is governed by {@link storm.mesos.resources.DefaultReservationTypeComparator}
   * {@param rangeResourceEntry} range resource to removeAndGet
   */
  @Override
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry) throws ResourceNotAvailableException {
    if (isAvailable(rangeResourceEntry)) {
      return removeAndGet(availableResourcesByReservationType.keySet(), rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, rangeResourceEntry, toString());
    throw new ResourceNotAvailableException(message);
  }


  /**
   * Remove/Reserve range from available ranges.
   * {@param rangeResourceEntry} range resource to removeAndGet
   * {@param reservationType} reservation type of resource that needs to be removed. If the resource represented by rangeResourceEntry
   * of the reservation type specified by this parameter is not available, then {@link ResourceNotAvailableException}
   * is thrown
   */
  @Override
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry, ReservationType reservationType) throws ResourceNotAvailableException {

    if (isAvailable(rangeResourceEntry, reservationType)) {
      List<ReservationType> reservationTypeList = new ArrayList<>();
      reservationTypeList.add(reservationType);
      return removeAndGet(reservationTypeList, rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' of reservationType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, reservationType.toString(), rangeResourceEntry.toString(), toString(availableResourcesByReservationType.get(reservationType)));
    throw new ResourceNotAvailableException(message);
  }

  /**
   * Remove/Reserve range from available ranges
   * {@param rangeResourceEntry} range resource to removeAndGet
   * {@param reservationTypeComparator} comparator like {@link storm.mesos.resources.DefaultReservationTypeComparator}
   * to determine the priority of reservation types. When resources of several reservation types ({@link storm.mesos.resources.ReservationType})
   * are available, the priority of reservation type removed is governed by {@link storm.mesos.resources.DefaultReservationTypeComparator}
   */
  @Override
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry, Comparator<ReservationType> reservationTypeCompartor) throws
    ResourceNotAvailableException {
    if (isAvailable(rangeResourceEntry)) {
      List<ReservationType> reservationTypeList = Arrays.asList(ReservationType.values());
      Collections.sort(reservationTypeList, reservationTypeCompartor);
      return removeAndGet(reservationTypeList, rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, rangeResourceEntry, toString());
    throw new ResourceNotAvailableException(message);
  }

  private List<ResourceEntry> removeAndGet(Collection<ReservationType> reservationTypes, RangeResourceEntry desiredRange) {
    List<ResourceEntry> removedResources = new ArrayList<>();
    Long desiredBegin = desiredRange.getBegin();
    Long desiredEnd = desiredRange.getEnd();

    for (ReservationType reservationType : reservationTypes) {
      List<RangeResourceEntry> availableRanges = availableResourcesByReservationType.get(reservationType);
      for (int i = 0; i < availableRanges.size(); i++) {
        RangeResourceEntry availableRange = availableRanges.get(i);
        if (desiredBegin >= availableRange.getBegin() && desiredEnd <= availableRange.getEnd()) {
          availableRanges.remove(i);
          // If this is exactly the ranges we were looking for, then we can use them
          if (availableRange.getBegin().equals(availableRange.getEnd()) || (availableRange.getBegin().equals(desiredBegin) && availableRange.getEnd().equals(desiredEnd))) {
            removedResources.add(availableRange);
            return removedResources;
          }
          // Salvage resources before the beginning of the requested range
          if (availableRange.getBegin() < desiredBegin) {
            availableRanges.add(new RangeResourceEntry(reservationType, availableRange.getBegin(), desiredBegin - 1));
          }
          // Salvage resources after the end of the requested range
          if (availableRange.getEnd() > desiredEnd) {
            availableRanges.add(new RangeResourceEntry(reservationType, desiredEnd + 1, availableRange.getEnd()));
          }
          // Now that we've salvaged all available resources, add the resources for the specifically requested range
          removedResources.add(new RangeResourceEntry(reservationType, desiredBegin, desiredEnd));
        }
      }
    }
    return removedResources;
  }

  public String toString(List<RangeResourceEntry> ranges) {
    List<String> rangeStrings = new ArrayList<>();
    for (RangeResourceEntry range : ranges) {
      String beginStr = String.valueOf(range.getBegin());
      String endStr = String.valueOf(range.getEnd());
      /*
      * A Range representing a single number still has both Range.begin
      * and Range.end populated, but they are set to the same value.
      * In that case we just return "N" instead of "N-N".
      */
      if (range.getBegin() == range.getEnd()) {
        rangeStrings.add(beginStr);
      } else {
        rangeStrings.add(String.format("%s-%s", beginStr, endStr));
      }
    }
    return String.format("%s: [%s]", resourceType.toString(), StringUtils.join(rangeStrings, ","));
  }

  public String toString(ReservationType reservationType) {
    return String.format("%s: %s", reservationType, toString(availableResourcesByReservationType.get(reservationType)));
  }

  public String toString() {
    List<RangeResourceEntry> resourceRanges = new ArrayList<>();
    for (Map.Entry<ReservationType, List<RangeResourceEntry>> entry : availableResourcesByReservationType.entrySet()) {
      resourceRanges.addAll(entry.getValue());
    }
    return toString(resourceRanges);
  }
}
