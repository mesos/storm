/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos.schedulers;

import com.google.common.base.Joiner;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos;
import storm.mesos.ResourceRoleComparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OfferResources {
  private final Logger log = Logger.getLogger(OfferResources.class);

  private class PortRange {
    public long begin;
    public long end;

    public PortRange(long begin, long end) {
      this.begin = begin;
      this.end = end;
    }
  }

  private List<Protos.Offer> offers;
  private String hostName;
  private double unreservedCpu;
  private double unreservedMem;
  private double reservedCpu;
  private double reservedMem;

  List<PortRange> portRanges = new ArrayList<>();

  public void addPortRanges(Protos.Value.Ranges ranges) {
    for (Protos.Value.Range r : ranges.getRangeList()) {
      this.portRanges.add(new PortRange(r.getBegin(), r.getEnd()));
    }
  }

  public OfferResources(Protos.Offer offer) {
    this.offers = new ArrayList<>();
    this.offers.add(offer);

    Protos.Value.Ranges portRanges = null;

    String hostName = offer.getHostname();

    for (Protos.Resource r : offer.getResourcesList()) {
      if (r.hasReservation()) {
        // skip resources with dynamic reservations
        continue;
      }

      if (r.getName().equals("cpus")) {
        if (!r.getRole().equals("*")) {
          reservedCpu += r.getScalar().getValue();
          continue;
        }
        unreservedCpu += r.getScalar().getValue();
      } else if (r.getName().equals("mem")) {
        if (!r.getRole().equals("*")) {
          reservedMem += r.getScalar().getValue();
          continue;
        }
        unreservedMem += r.getScalar().getValue();
      } else if (r.getName().equals("ports")) {
        Protos.Value.Ranges tmp = r.getRanges();
        if (portRanges == null) {
          portRanges = tmp;
          continue;
        }
        portRanges.getRangeList().addAll(tmp.getRangeList());
      }
    }

    this.hostName = hostName;
    if ((portRanges != null) && (!portRanges.getRangeList().isEmpty())) {
      this.addPortRanges(portRanges);
    }
  }

  public void merge(Protos.Offer offer) {
    this.offers.add(offer);

    Protos.Value.Ranges portRanges = null;

    String hostName = offer.getHostname();
    if (!this.hostName.equals(hostName)) {
      log.error("OfferConsolidationError: Offer from " + this.hostName + " should not be merged with " + hostName);
      return;
    }

    // TODO(ksoundararaj) : dry this code
    for (Protos.Resource r : offer.getResourcesList()) {
      if (r.getName().equals("cpus")) {
        if (!r.getRole().equals("*")) {
          reservedCpu += r.getScalar().getValue();
          continue;
        }
        unreservedMem += r.getScalar().getValue();
      } else if (r.getName().equals("mem")) {
        if (!r.getRole().equals("*")) {
          reservedMem += r.getScalar().getValue();
          continue;
        }
        unreservedMem += r.getScalar().getValue();
      } else if (r.getName().equals("ports")) {
        portRanges = r.getRanges();
      }
    }

    if ((portRanges != null) && (!portRanges.getRangeList().isEmpty())) {
      this.addPortRanges(portRanges);
    }
  }

  public List<Protos.Resource> getResourcesListScalar(final double value, final String name, String frameworkRole) {

    double valueNeeded = value;
    List<Protos.Resource> retVal = new ArrayList<>();

    switch (name) {
      case "cpu":
        if ((unreservedCpu + reservedCpu) < valueNeeded) {
          // TODO(ksoundararaj): Throw ResourceUnavailableException
          return null;
        }
        double tmp = 0.0d;
        if (reservedCpu > valueNeeded) {
          tmp = valueNeeded;
          reservedCpu -= valueNeeded;
          valueNeeded = 0;
        } else if (reservedCpu < valueNeeded) {
          tmp = reservedCpu;
          reservedCpu = 0;
          valueNeeded -= reservedCpu;
        }
        retVal.add(Protos.Resource.newBuilder()
                                  .setName(name)
                                  .setType(Protos.Value.Type.SCALAR)
                                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(tmp))
                                  .setRole(frameworkRole)
                                  .build());

        if (valueNeeded > 0) {
          unreservedCpu -= valueNeeded;
        }
        retVal.add(Protos.Resource.newBuilder()
                                  .setName(name)
                                  .setType(Protos.Value.Type.SCALAR)
                                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(valueNeeded))
                                  .setRole(frameworkRole)
                                  .build());
        break;
      case "mem":
        if ((unreservedMem + reservedMem) < valueNeeded) {
          // TODO(ksoundararaj): Throw ResourceUnavailableException
          return null;
        }
        tmp = 0;
        if (reservedMem > valueNeeded) {
          tmp = valueNeeded;
          reservedMem -= valueNeeded;
          valueNeeded = 0;
        } else if (reservedMem < valueNeeded) {
          tmp = reservedMem;
          reservedMem = 0;
          valueNeeded -= reservedMem;
        }
        retVal.add(Protos.Resource.newBuilder()
                                  .setName(name)
                                  .setType(Protos.Value.Type.SCALAR)
                                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(tmp))
                                  .setRole(frameworkRole)
                                  .build());

        if (valueNeeded > 0) {
          unreservedMem -= valueNeeded;
        }
        retVal.add(Protos.Resource.newBuilder()
                                  .setName(name)
                                  .setType(Protos.Value.Type.SCALAR)
                                  .setScalar(Protos.Value.Scalar.newBuilder().setValue(valueNeeded))
                                  .setRole(frameworkRole)
                                  .build());
        break;
      default:
        return null;
    }
    return retVal;
  }


  public List<Protos.Resource> getResourcesRange(final long value, final String name) {
    List<Protos.Resource> resourceList = getResourceList();
    List<Protos.Resource> retVal = null;

    if (name.equals("ports")) {
      long portNumber = getPort(value);
      if (portNumber == -1) {
        // TODO(ksoundararaj) : Throw ResourceNotAvailableException instead of returning null
        return null;
      }
      for (Protos.Resource r : resourceList) {
        if (r.getType() == Protos.Value.Type.RANGES && r.getName().equals(name)) {
          for (Protos.Value.Range range : r.getRanges().getRangeList()) {
            if (value >= range.getBegin() && value <= range.getEnd()) {
              retVal = Arrays.asList(r.toBuilder()
                                      .setRanges(
                                        Protos.Value.Ranges.newBuilder()
                                                           .addRange(
                                                             Protos.Value.Range.newBuilder().setBegin(value).setEnd(value).build()
                                                           ).build()
                                      ).build()
              );
            }
          }
        }
      }
    }
    return retVal;
  }

  // TODO(ksoundararaj): Dry this code
  public List<Protos.Resource> getResourcesRange(final String name) {
    List<Protos.Resource> resourceList = getResourceList();
    List<Protos.Resource> retVal = null;

    if (name.equals("ports")) {
      long portNumber = getPort();
      for (Protos.Resource r : resourceList) {
        if (r.getType() == Protos.Value.Type.RANGES && r.getName().equals(name)) {
          for (Protos.Value.Range range : r.getRanges().getRangeList()) {
            if (portNumber >= range.getBegin() && portNumber <= range.getEnd()) {
              retVal = Arrays.asList(r.toBuilder()
                                      .setRanges(
                                        Protos.Value.Ranges.newBuilder()
                                                           .addRange(
                                                             Protos.Value.Range.newBuilder().setBegin(portNumber).setEnd(portNumber).build()
                                                           ).build()
                                      ).build()
              );
            }
          }
        }
      }
    }
    return retVal;
  }

  public List<Protos.Resource> getResourceList() {
    List<Protos.Resource> availableResources = new ArrayList<>();
    for (Protos.Offer offer : offers) {
      availableResources.addAll(offer.getResourcesList());
    }
    Collections.sort(availableResources, new ResourceRoleComparator());
    return availableResources;
  }

  public Protos.SlaveID getSlaveId() {
    return offers.get(0).getSlaveId();
  }

  public List<Protos.OfferID> getOfferIds() {
    List<Protos.OfferID> offerIDList = new ArrayList<>();

    for (Protos.Offer offer : offers) {
      offerIDList.add(offer.getId());
    }
    return offerIDList;
  }

  public String getHostName() {
    return this.hostName;
  }

  public double getMem() {
    return this.reservedMem + this.unreservedMem;
  }

  public double getCpu() {
    return this.reservedCpu + this.unreservedCpu;
  }


  public double decMem(double value) {
    // TODO(ksoundararaj) : Make sure the value doesnt go -ve
    if (reservedMem > value) {
      reservedMem -= value;
    } else if (reservedMem < value) {
      reservedMem = 0;
      unreservedMem -= value - reservedMem;
    }
    return this.reservedMem + this.unreservedMem;
  }

  public double decCpu(double value) {
    // TODO(ksoundararaj) : Make sure the value doesnt go -ve
    if (reservedCpu > value) {
      reservedCpu -= value;
    } else if (reservedCpu < value) {
      reservedCpu = 0;
      unreservedCpu -= value - reservedCpu;
    }
    return this.reservedCpu + this.unreservedCpu;
  }

  public long getPort() {
    if (!hasPort()) {
      return -1;
    }

    for (int i = 0; i < portRanges.size(); i++) {
      PortRange portRange = portRanges.get(i);
      if (portRange.begin < portRange.end) {
        return portRange.begin++;
      } else if (portRange.begin == portRange.end) {
        portRanges.remove(i);
        return portRange.begin;
      }
    }

    return -1;
  }

  public long getPort(long portNumber) {
    boolean portFound = false;

    for (int i = 0; i < portRanges.size(); i++) {
      PortRange portRange = portRanges.get(i);

      if (portNumber > portRange.begin && portNumber < portRange.end) {
        portRanges.add(new PortRange(portRange.begin, portNumber - 1));
        portRanges.add(new PortRange(portNumber - 1, portRange.end));
        portFound = true;
      } else if (portRange.begin == portRange.end) {
        portFound = true;
      }

      if (portFound) {
        portRanges.remove(i);
        return portNumber;
      }
    }

    return -1;
  }

  public boolean hasPort() {
    return (portRanges != null && !portRanges.isEmpty());
  }

  private String offersToString(List<Protos.Offer> offers) {
    List<String> offerIds = new ArrayList<>();
    for (Protos.Offer offer : offers) {
      offerIds.add(offer.getId().getValue());
    }
    return "[" + Joiner.on(".").join(offerIds) + "]";
  }

  private String portRangesToString(List<PortRange> portRanges) {
    List<String> portRangeStrings = new ArrayList<>();

    for (int i = 0; i < portRanges.size(); i++) {
      if (portRanges.get(i).begin == portRanges.get(i).end) {
        portRangeStrings.add(String.valueOf(portRanges.get(i).begin));
      } else {
        portRangeStrings.add(String.valueOf(portRanges.get(i).begin) + "-" + String.valueOf(portRanges.get(i).end));
      }
    }

    return "[" + Joiner.on(",").join(portRangeStrings) + "]";
  }

  @Override
  public String toString() {
    return "OfferResources with offerIds: " + offersToString(offers) + " host: " + getHostName() + " mem: " + String.valueOf(unreservedMem + reservedMem) +
           " cpu: " + String.valueOf(unreservedCpu + reservedCpu) +
           " portRanges: " + portRangesToString(portRanges);
  }
}

