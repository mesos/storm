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

import backtype.storm.scheduler.TopologyDetails;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.PrettyProtobuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregatedOffers {

  private final Logger log = LoggerFactory.getLogger(AggregatedOffers.class);
  private Map<ResourceType, Resource> availableResources;

  private List<Protos.Offer> offerList = new ArrayList<Protos.Offer>();

  private final String hostName;

  private Protos.SlaveID slaveID;

  private void initializeAvailableResources() {
    availableResources = new HashMap<>();
    availableResources.put(ResourceType.CPU, new ScalarResource(ResourceType.CPU));
    availableResources.put(ResourceType.MEM, new ScalarResource(ResourceType.MEM));
    availableResources.put(ResourceType.PORTS, new RangeResource(ResourceType.PORTS));
  }

  public AggregatedOffers(String hostName) {
    this.hostName = hostName;
    initializeAvailableResources();
  }

  public AggregatedOffers(Protos.Offer offer) {
    initializeAvailableResources();
    this.slaveID = offer.getSlaveId();
    this.hostName = offer.getHostname();
    add(offer);
  }

  public String getHostName() {
    return hostName;
  }

  public void add(Protos.Offer offer) {
    offerList.add(offer);

    for (Protos.Resource r : offer.getResourcesList()) {
      ResourceType resourceType = ResourceType.of(r.getName());
      ReservationType reservationType = (r.getRole().equals("*")) ?
                                        ReservationType.UNRESERVED : ReservationType.STATIC;

      if (r.hasReservation()) {
        // skip resources with dynamic reservations
        continue;
      }

      switch (resourceType) {
        case CPU:
        case MEM:
          ResourceEntries.ScalarResourceEntry scalarResourceEntry = new ResourceEntries.ScalarResourceEntry(reservationType, r.getScalar().getValue());
          availableResources.get(resourceType).add(scalarResourceEntry, reservationType);
          break;
        case PORTS:
          for (Protos.Value.Range range : r.getRanges().getRangeList()) {
            ResourceEntries.RangeResourceEntry rangeResourceEntry = new ResourceEntries.RangeResourceEntry(reservationType, range.getBegin(), range.getEnd());
            availableResources.get(resourceType).add(rangeResourceEntry, reservationType);
          }
          break;
        case DISK:
          // TODO: Support disk resource isolation (https://github.com/mesos/storm/issues/147)
          break;
        default:
          log.warn(String.format("Found unsupported resourceType '%s' while adding offer %s", resourceType, PrettyProtobuf.offerToString(offer)));
      }
    }
  }

  public boolean isAvailable(ResourceType resourceType, ResourceEntry<?> resource) {
    return availableResources.get(resourceType).isAvailable(resource);
  }

  public boolean isAvailable(ResourceType resourceType, ReservationType reservationType, ResourceEntry<?> resource) {
    return availableResources.get(resourceType).isAvailable(resource, reservationType);
  }

  public <T extends ResourceEntry> List<T> getAllAvailableResources(ResourceType resourceType) {
    return availableResources.get(resourceType).getAllAvailableResources();
  }

  public <T extends ResourceEntry> List<T> getAllAvailableResources(ResourceType resourceType, ReservationType reservationType) {
    return availableResources.get(resourceType).getAllAvailableResources(reservationType);
  }

  public void reserve(ResourceType resourceType, ResourceEntry<?> resource) throws ResourceNotAvailableException {
    if (availableResources.get(resourceType).isAvailable(resource)) {
      availableResources.get(resourceType).removeAndGet(resource);
    }
  }

  public List<ResourceEntry> reserveAndGet(ResourceType resourceType, ResourceEntry<?> resource) throws ResourceNotAvailableException {
    if (availableResources.get(resourceType).isAvailable(resource)) {
      return availableResources.get(resourceType).removeAndGet(resource);
    }
    return new ArrayList<>();
  }

  public List<ResourceEntry> reserveAndGet(ResourceType resourceType, ReservationType reservationType, ResourceEntry<?> resource) throws
    ResourceNotAvailableException {
    if (availableResources.get(resourceType).isAvailable(resource, reservationType)) {
      return availableResources.get(resourceType).removeAndGet(resource, reservationType);
    }
    return new ArrayList<>();
  }

  public List<Protos.Offer> getOfferList() {
    return offerList;
  }

  public List<Protos.OfferID> getOfferIDList() {
    List<Protos.OfferID> offerIDList = new ArrayList<>();
    for (Protos.Offer offer: offerList) {
      offerIDList.add(offer.getId());
    }
    return offerIDList;
  }

  public Protos.SlaveID getSlaveID() {
    return slaveID;
  }

  @Override
  public String toString() {
    return String.format("%s, %s, %s",
                         availableResources.get(ResourceType.CPU),
                         availableResources.get(ResourceType.MEM),
                         availableResources.get(ResourceType.PORTS));
  }


  public boolean isFit(Map mesosStormConf, TopologyDetails topologyDetails, boolean supervisorExists) {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    requestedWorkerCpu += supervisorExists ? 0 : MesosCommon.executorCpu(mesosStormConf);
    requestedWorkerMem += supervisorExists ? 0 : MesosCommon.executorMem(mesosStormConf);

    return (isAvailable(ResourceType.CPU, new ResourceEntries.ScalarResourceEntry(requestedWorkerCpu)) &&
            isAvailable(ResourceType.MEM, new ResourceEntries.ScalarResourceEntry(requestedWorkerMem)) &&
            !getAllAvailableResources(ResourceType.PORTS).isEmpty());
  }

  public boolean isFit(Map mesosStormConf, TopologyDetails topologyDetails, Long port, boolean supervisorExists) {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    requestedWorkerCpu += supervisorExists ? 0 : MesosCommon.executorCpu(mesosStormConf);
    requestedWorkerMem += supervisorExists ? 0 : MesosCommon.executorMem(mesosStormConf);

    return (isAvailable(ResourceType.CPU, new ResourceEntries.ScalarResourceEntry(requestedWorkerCpu)) &&
            isAvailable(ResourceType.MEM, new ResourceEntries.ScalarResourceEntry(requestedWorkerMem)) &&
            isAvailable(ResourceType.PORTS, new ResourceEntries.RangeResourceEntry(port, port)));
  }
}

