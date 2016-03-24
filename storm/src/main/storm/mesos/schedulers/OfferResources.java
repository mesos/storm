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

import com.google.common.base.Joiner;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.apache.mesos.Protos;

import java.util.ArrayList;
import java.util.List;

public class OfferResources {
  private final Logger log = LoggerFactory.getLogger(OfferResources.class);

  private class PortRange {
    public long begin;
    public long end;

    public PortRange(long begin, long end) {
      this.begin = begin;
      this.end = end;
    }
  }

  private Protos.Offer offer;
  private Protos.OfferID offerId;
  private String hostName;
  private double mem;
  private double cpu;

  List<PortRange> portRanges = new ArrayList<>();

  public void addPortRanges(Protos.Value.Ranges ranges) {
    for (Protos.Value.Range r : ranges.getRangeList()) {
      this.portRanges.add(new PortRange(r.getBegin(), r.getEnd()));
    }
  }

  public OfferResources(Protos.Offer offer) {
    this.offer = offer;
    this.offerId = offer.getId();
    double offerMem = 0;
    double offerCpu = 0;
    Protos.Value.Ranges portRanges = null;

    String hostName = offer.getHostname();
    for (Protos.Resource r : offer.getResourcesList()) {
      if (r.hasReservation()) {
        // skip resources with dynamic reservations
        continue;
      }
      if (r.getName().equals("cpus")) {
        offerCpu += r.getScalar().getValue();
      } else if (r.getName().equals("mem")) {
        offerMem += r.getScalar().getValue();
      } else if (r.getName().equals("ports")) {
        Protos.Value.Ranges tmp  = r.getRanges();
        if (portRanges == null) {
          portRanges = tmp;
          continue;
        }
        portRanges.getRangeList().addAll(tmp.getRangeList());
      }
    }

    this.hostName = hostName;
    this.mem = offerMem;
    this.cpu = offerCpu;
    if ((portRanges != null) && (!portRanges.getRangeList().isEmpty())) {
      this.addPortRanges(portRanges);
    }
  }

  public Protos.Offer getOffer() {
    return this.offer;
  }

  public Protos.OfferID getOfferId() {
    return this.offerId;
  }

  public String getHostName() {
    return this.hostName;
  }

  public double getMem() {
    return this.mem;
  }

  public double getCpu() {
    return this.cpu;
  }

  public void decCpu(double val) {
    cpu -= val;
  }

  public void decMem(double val) {
    mem -= val;
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

  public boolean hasPort() {
    return (portRanges != null && !portRanges.isEmpty());
  }

  @Override
  public String toString() {
    List<String> portRangeStrings = new ArrayList<>();

    for (int i = 0; i < portRanges.size(); i++) {
      if (portRanges.get(i).begin == portRanges.get(i).end) {
        portRangeStrings.add(String.valueOf(portRanges.get(i).begin));
      } else {
        portRangeStrings.add(String.valueOf(portRanges.get(i).begin) + "-" + String.valueOf(portRanges.get(i).end));
      }
    }
    return "OfferResources with offerId: " + getOfferId().getValue().toString().trim() + " from host: " + getHostName() + " mem: " + String.valueOf(mem) +
           " cpu: " + String.valueOf(cpu) +
           " portRanges: [" + Joiner.on(",").join(portRangeStrings) + "]";
  }
}

