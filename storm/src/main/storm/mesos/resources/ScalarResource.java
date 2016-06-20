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

import static storm.mesos.resources.ResourceEntries.ScalarResourceEntry;

public class ScalarResource implements Resource<ScalarResourceEntry> {
  public final ResourceType resourceType;
  private final Map<ReservationType, ScalarResourceEntry> availableResourcesByReservationType;
  Double totalAvailableResource = 0.0;


  public ScalarResource(ResourceType resourceType) {
    this.resourceType = resourceType;
    availableResourcesByReservationType = new TreeMap<>(new DefaultReservationTypeComparator());
    for (ReservationType reservationType : ReservationType.values()) {
      availableResourcesByReservationType.put(reservationType, new ScalarResourceEntry(reservationType, 0.0));
    }
  }

  public boolean isAvailable(ScalarResourceEntry scalarResourceEntry) {
    return (totalAvailableResource >= scalarResourceEntry.getValue());
  }

  public boolean isAvailable(ScalarResourceEntry scalarResourceEntry, ReservationType reservationType) {
    return (availableResourcesByReservationType.get(reservationType).getValue() >= scalarResourceEntry.getValue());
  }

  public Double getTotalAvailableResource(ReservationType reservationType) {
    return availableResourcesByReservationType.get(reservationType).getValue();
  }

  public List<ScalarResourceEntry> getAllAvailableResources() {
    List<ScalarResourceEntry> retVal = new ArrayList<>();
    for (ScalarResourceEntry scalarResourceEntry : availableResourcesByReservationType.values()) {
      retVal.add(scalarResourceEntry);
    }
    return retVal;
  }

  public List<ScalarResourceEntry> getAllAvailableResources(ReservationType reservationType) {
    List<ScalarResourceEntry> retVal = new ArrayList<>();
    retVal.add(availableResourcesByReservationType.get(reservationType));
    return retVal;
  }

  public void add(ScalarResourceEntry scalarResourceEntry, ReservationType reservationType) {
    if (scalarResourceEntry.getValue() <= 0) {
      return;
    }
    availableResourcesByReservationType.get(reservationType).add(scalarResourceEntry);
    totalAvailableResource += scalarResourceEntry.getValue();
  }

  public List<ResourceEntry> removeAndGet(ScalarResourceEntry scalarResourceEntry) throws ResourceNotAvailableException {
    return removeAndGet(scalarResourceEntry, availableResourcesByReservationType.keySet());
  }

  public List<ResourceEntry> reserveScalarResource(ResourceType resourceType, ScalarResourceEntry requiredValue) throws ResourceNotAvailableException {
    if (totalAvailableResource < requiredValue.getValue()) {
      throw new ResourceNotAvailableException(String.format("resourceType: {} is not available. Requested {} Available {}",
                                                           resourceType, requiredValue, totalAvailableResource));
    }
    return removeAndGet(requiredValue);
  }

  /**
   * Removes/Reserves scalar resource from available resources.
   * {@param scalarResourceEntry} amount of scalar resource to removeAndGet/decrement
   * {@param reservationTypeComparator} comparator like {@link storm.mesos.resources.DefaultReservationTypeComparator}
   * to determine the priority of the reservation type. When resources of all reservations are available, resources
   * are removed in the priority order specified by this comparator.
   */
  public List<ResourceEntry> removeAndGet(ScalarResourceEntry scalarResourceEntry, Comparator<ReservationType> reservationTypeComparator) throws
    ResourceNotAvailableException {
    List<ReservationType> reservationTypeList = Arrays.asList(ReservationType.values());
    Collections.sort(reservationTypeList, reservationTypeComparator);
    return removeAndGet(scalarResourceEntry, reservationTypeList);
  }

  /**
   * Removes/Reserves scalar resource from available resources. When resources of all reservations are available, resources
   * are removed in the priority order specified by {@link storm.mesos.resources.DefaultReservationTypeComparator}
   * {@param scalarResourceEntry} amount of scalar resource to removeAndGet/decrement.
   * {@link storm.mesos.resources.DefaultReservationTypeComparator} determines the priority of the reservation type.
   */
  public List<ResourceEntry> removeAndGet(ScalarResourceEntry scalarResourceEntry, ReservationType reservationType) throws ResourceNotAvailableException {
    ScalarResourceEntry availableResource = availableResourcesByReservationType.get(reservationType);
    List<ResourceEntry> reservedResources = new ArrayList<>();

    if (scalarResourceEntry.getValue() <= availableResource.getValue()) {
      availableResourcesByReservationType.put(reservationType, availableResource.remove(scalarResourceEntry));
      totalAvailableResource -= scalarResourceEntry.getValue();
      reservedResources.add(new ScalarResourceEntry(scalarResourceEntry.getReservationType(), scalarResourceEntry.getValue()));
      return reservedResources;
    }
    String message = String.format("ResourceType '%s' of reservationType '%s' is not available. Requested value: %s Available: %s",
                                   resourceType, reservationType.toString(), scalarResourceEntry.getValue(), availableResourcesByReservationType.get(reservationType));
    throw new ResourceNotAvailableException(message);
  }

  public String toString() {
    List<String> availableResourcesByResourceTypeList = new ArrayList<>();
    for (Map.Entry<ReservationType, ScalarResourceEntry> entry: availableResourcesByReservationType.entrySet()) {
      availableResourcesByResourceTypeList.add(String.format("%s: %f", entry.getKey(), entry.getValue().getValue()));
    }
    String tmp = StringUtils.join(availableResourcesByResourceTypeList, ", ");
    return String.format("%s: %f (%s)", resourceType.toString(), totalAvailableResource, tmp);
  }

  private List<ResourceEntry> removeAndGet(ScalarResourceEntry scalarResourceEntry, Collection<ReservationType> reservationTypesListByPriority) throws
    ResourceNotAvailableException {
    Double requiredValue = scalarResourceEntry.getValue();
    List<ResourceEntry> reservedResources = new ArrayList<>();

    if (requiredValue > totalAvailableResource) {
      String message = String.format("ResourceType '%s' is not available. Requested value: %s Available: %s",
                                     resourceType, requiredValue, totalAvailableResource);
      throw new ResourceNotAvailableException(message);
    }

    for (ReservationType reservationType : reservationTypesListByPriority) {
      ScalarResourceEntry availableResource = availableResourcesByReservationType.get(reservationType);
      Double availableResourceValue = availableResource.getValue();

      if (availableResourceValue >= requiredValue) {
        availableResource.remove(new ScalarResourceEntry(requiredValue));
        totalAvailableResource -= requiredValue;
        reservedResources.add(new ScalarResourceEntry(reservationType, requiredValue));
        return reservedResources;
      } else if (availableResourceValue > 0) {
        // Fact that we are here => 0 < availableResourceValue < requiredValue.
        // So we could entire availableResourceValue.
        availableResourcesByReservationType.put(reservationType, new ScalarResourceEntry(reservationType, 0.0));
        requiredValue -= availableResourceValue;
        totalAvailableResource -= availableResourceValue;
        reservedResources.add(new ScalarResourceEntry(reservationType, availableResourceValue));
      }
    }
    return reservedResources;
  }
}
