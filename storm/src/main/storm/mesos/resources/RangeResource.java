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
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry) throws ResourceNotAvailabeException {
    if (isAvailable(rangeResourceEntry)) {
      return removeAndGet(availableResourcesByReservationType.keySet(), rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, rangeResourceEntry, toString());
    throw new ResourceNotAvailabeException(message);
  }


  /**
   * Remove/Reserve range from available ranges.
   * {@param rangeResourceEntry} range resource to removeAndGet
   * {@parm reservationType} reservation type of resource that needs to be removed. If the resource represented by rangeResourceEntry
   * of the reservation type specied by this parameter is not available, then {@link storm.mesos.resources.ResourceNotAvailabeException}
   * is thrown
   */
  @Override
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry, ReservationType reservationType) throws ResourceNotAvailabeException {

    if (isAvailable(rangeResourceEntry, reservationType)) {
      List<ReservationType> reservationTypeList = new ArrayList<>();
      reservationTypeList.add(reservationType);
      return removeAndGet(reservationTypeList, rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' of reservationType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, reservationType.toString(), rangeResourceEntry.toString(), toString(availableResourcesByReservationType.get(reservationType)));
    throw new ResourceNotAvailabeException(message);
  }

  /**
   * Remove/Reserve range from available ranges
   * {@param rangeResourceEntry} range resource to removeAndGet
   * {@param reservationTypeComparator} comparator like {@link storm.mesos.resources.DefaultReservationTypeComparator}
   * to determine the priority of reservation types. When resources of several reservation types ({@link storm.mesos.resources.ReservationType})
   * are available, the priority of reservation type removed is governed by {@link storm.mesos.resources.DefaultReservationTypeComparator}
   */
  @Override
  public List<ResourceEntry> removeAndGet(RangeResourceEntry rangeResourceEntry, Comparator<ReservationType> reservationTypeCompartor) throws ResourceNotAvailabeException {
    if (isAvailable(rangeResourceEntry)) {
      List<ReservationType> reservationTypeList = Arrays.asList(ReservationType.values());
      Collections.sort(reservationTypeList, reservationTypeCompartor);
      return removeAndGet(reservationTypeList, rangeResourceEntry);
    }

    String message = String.format("ResourceType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, rangeResourceEntry, toString());
    throw new ResourceNotAvailabeException(message);
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
    return String.format("[%s]", StringUtils.join(rangeStrings, ","));
  }

  public String toString(ReservationType reservationType) {
    return String.format("%s : %s", reservationType, toString(availableResourcesByReservationType.get(reservationType)));
  }

  public String toString() {
    List<RangeResourceEntry> resourceRanges = new ArrayList<>();
    for (Map.Entry<ReservationType, List<RangeResourceEntry>> entry : availableResourcesByReservationType.entrySet()) {
      resourceRanges.addAll(entry.getValue());
    }
    return toString(resourceRanges);
  }

  private List<ResourceEntry> removeAndGet(Collection<ReservationType> reservationTypes, RangeResourceEntry rangeResourceEntry) {
    List<ResourceEntry> removedResources = new ArrayList<>();
    Long beginValue = rangeResourceEntry.getBegin();
    Long endValue = rangeResourceEntry.getEnd();

    for (ReservationType reservationType : reservationTypes) {
      List<RangeResourceEntry> rangeResourceEntryList = availableResourcesByReservationType.get(reservationType);
      for (int i = 0; i < rangeResourceEntryList.size(); i++) {
        RangeResourceEntry tmp = rangeResourceEntryList.get(i);
        if (beginValue >= tmp.getBegin() && endValue <= tmp.getEnd()) {
          rangeResourceEntryList.remove(i);
          // We already removed the entry. So when beginValue == endValue,
          // we dont have to add a new entry
          if (tmp.getBegin().equals(tmp.getEnd()) || (tmp.getBegin().equals(beginValue) && tmp.getEnd().equals(endValue))) {
            removedResources.add(tmp);
            return removedResources;
          }

          if (beginValue > tmp.getBegin() && tmp.getEnd().equals(endValue)) {
            rangeResourceEntryList.add(new RangeResourceEntry(reservationType, tmp.getBegin(), beginValue - 1));
            removedResources.add(new RangeResourceEntry(reservationType, beginValue, endValue));
          } else if (tmp.getBegin().equals(beginValue) && endValue < tmp.getEnd()) {
            rangeResourceEntryList.add(new RangeResourceEntry(reservationType, endValue + 1, tmp.getEnd()));
            removedResources.add(new RangeResourceEntry(reservationType, beginValue, endValue));
          } else if (beginValue > tmp.getBegin() && endValue < tmp.getEnd()) {
            rangeResourceEntryList.add(new RangeResourceEntry(reservationType, tmp.getBegin(), beginValue - 1));
            rangeResourceEntryList.add(new RangeResourceEntry(reservationType, endValue + 1, tmp.getEnd()));
            removedResources.add(new RangeResourceEntry(reservationType, beginValue, endValue));
          }
        }
      }
    }
    return removedResources;
  }
}
