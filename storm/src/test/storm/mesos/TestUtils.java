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
package storm.mesos;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.mesos.Protos;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ReservationType;
import storm.mesos.resources.ResourceEntries;
import storm.mesos.resources.ResourceEntry;
import storm.mesos.resources.ResourceType;
import storm.mesos.util.MesosCommon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

  public static Map initializeStormTopologyConfig(Map conf) {
    conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, 768.0);
    conf.put(Config.TOPOLOGY_PRIORITY, 0);
    return conf;
  }

  public static TopologyDetails constructTopologyDetails(String topologyName, int numWorkers) {
    return new TopologyDetails(topologyName, initializeStormTopologyConfig(new HashMap<>()), new StormTopology(), numWorkers, "root");
  }

  public static TopologyDetails constructTopologyDetails(String topologyName, int numWorkers, double numCpus, double memSize) {
    TopologyDetails topologyDetails = constructTopologyDetails(topologyName, numWorkers);

    topologyDetails.getConf().put(MesosCommon.WORKER_CPU_CONF, Double.valueOf(numCpus));
    topologyDetails.getConf().put(MesosCommon.WORKER_MEM_CONF, Double.valueOf(memSize));

    return topologyDetails;
  }

  public static Protos.Offer buildOffer(String offerId, String hostName, double cpus, double mem) {
    return Protos.Offer.newBuilder()
                       .setId(Protos.OfferID.newBuilder().setValue(String.valueOf(offerId)).build())
                       .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("derp").build())
                       .setSlaveId(Protos.SlaveID.newBuilder().setValue("derp").build())
                       .setHostname(hostName)
                       .addAllResources(
                         Arrays.asList(
                           buildScalarResource("cpus", cpus),
                           buildScalarResourceWithDynamicReservation("cpus", 1.0, "dynamicallyReserved"),
                           buildScalarResourceWithDynamicReservation("mem", 1.0, "dynamicallyReserved"),
                           buildScalarResource("mem", mem)
                         )
                       )
                       .build();
  }

  public static Protos.Offer buildOfferWithPorts(String offerId, String hostName, double cpus, double mem, int portBegin, int portEnd) {
    return Protos.Offer.newBuilder()
                       .setId(Protos.OfferID.newBuilder().setValue(offerId).build())
                       .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("derp").build())
                       .setSlaveId(Protos.SlaveID.newBuilder().setValue("derp").build())
                       .setHostname(hostName)
                       .addAllResources(
                         Arrays.asList(
                           buildScalarResource("cpus", cpus),
                           buildScalarResourceWithDynamicReservation("cpus", 1.0, "dynamicallyReserved"),
                           buildScalarResource("mem", mem),
                           buildRangeResource("ports", portBegin, portEnd)
                         )
                       )
                       .build();
  }

  public static Protos.Offer buildOfferWithReservation(String offerId, String hostName, double cpus, double mem, double reservedCpu, double reservedMem) {
    return Protos.Offer.newBuilder()
                       .setId(Protos.OfferID.newBuilder().setValue(offerId).build())
                       .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("derp").build())
                       .setSlaveId(Protos.SlaveID.newBuilder().setValue("derp").build())
                       .setHostname(hostName)
                       .addAllResources(
                         Arrays.asList(
                           buildScalarResource("cpus", cpus),
                           buildScalarResourceWithDynamicReservation("cpus", 1.0, "dynamicallyReserved"),
                           buildScalarResource("mem", mem),
                           buildScalarResourceWithRole("cpus", reservedCpu, "reserved"),
                           buildScalarResourceWithRole("mem", reservedMem, "reserved")
                         )
                       )
                       .build();
  }

  public static Protos.Offer buildOfferWithReservationAndPorts(String offerId, String hostName, double cpus,
                                                               double mem, double reservedCpu, double reservedMem,
                                                               int portBegin, int portEnd) {
    return Protos.Offer.newBuilder()
                       .setId(Protos.OfferID.newBuilder().setValue(offerId).build())
                       .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("derp").build())
                       .setSlaveId(Protos.SlaveID.newBuilder().setValue("derp").build())
                       .setHostname(hostName)
                       .addAllResources(
                         Arrays.asList(
                           buildScalarResource("cpus", cpus),
                           buildScalarResourceWithDynamicReservation("cpus", 1.0, "dynamicallyReserved"),
                           buildScalarResource("mem", mem),
                           buildScalarResourceWithRole("cpus", reservedCpu, "reserved"),
                           buildScalarResourceWithRole("mem", reservedMem, "reserved"),
                           buildRangeResource("ports", portBegin, portEnd)
                         )
                       )
                       .build();
  }

  public static Protos.Resource buildScalarResource(String name, double value) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.SCALAR)
                          .setScalar(Protos.Value.Scalar.newBuilder()
                                                        .setValue(value)
                                                        .build())
                          .setName(name)
                          .build();
  }

  public static Protos.Resource buildScalarResourceWithRole(String name, double value, String role) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.SCALAR)
                          .setScalar(Protos.Value.Scalar.newBuilder()
                                                        .setValue(value)
                                                        .build())
                          .setName(name)
                          .setRole(role)
                          .build();
  }

  public static Protos.Resource buildScalarResourceWithDynamicReservation(String name, double value, String role) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.SCALAR)
                          .setScalar(Protos.Value.Scalar.newBuilder()
                                                        .setValue(value)
                                                        .build())
                          .setName(name)
                          .setRole(role)
                          .setReservation(
                            Protos.Resource.ReservationInfo.newBuilder()
                                                           .setPrincipal("derp")
                                                           .build()
                          )
                          .build();
  }

  public static List<Protos.Resource> buildResourceList(double cpus, double mem, double reservedCpu, double reservedMem) {
    List<Protos.Resource> resourceList = new ArrayList<>();
    resourceList.addAll(
      Arrays.asList(
        buildScalarResource("cpus", cpus),
        buildScalarResource("mem", mem),
        buildScalarResourceWithRole("cpus", reservedCpu, "reserved"),
        buildScalarResourceWithRole("mem", reservedMem, "reserved")
      )
    );
    return resourceList;
  }

  public static Protos.Resource buildRangeResource(String name, int begin, int end) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.RANGES)
                          .setRanges(
                            Protos.Value.Ranges.newBuilder()
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin)
                                                                           .setEnd(end)
                                                                           .build())
                                               .build()
                          )
                          .setName(name)
                          .build();
  }

  public static Protos.Resource buildRangeResourceWithRole(String name, int begin, int end, String role) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.RANGES)
                          .setRanges(
                            Protos.Value.Ranges.newBuilder()
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin)
                                                                           .setEnd(end)
                                                                           .build())
                                               .build()
                          )
                          .setName(name)
                          .setRole(role)
                          .build();
  }

  public static Protos.Resource buildMultiRangeResource(String name, int begin1, int end1, int begin2, int end2) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.RANGES)
                          .setRanges(
                            Protos.Value.Ranges.newBuilder()
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin1)
                                                                           .setEnd(end1)
                                                                           .build())
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin2)
                                                                           .setEnd(end2)
                                                                           .build())
                                               .build()
                          )
                          .setName(name)
                          .build();
  }

  public static Protos.Resource buildMultiRangeResourceWithRole(String name, int begin1, int end1, int begin2, int end2, String role) {
    return Protos.Resource.newBuilder()
                          .setType(Protos.Value.Type.RANGES)
                          .setRanges(
                            Protos.Value.Ranges.newBuilder()
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin1)
                                                                           .setEnd(end1)
                                                                           .build())
                                               .addRange(Protos.Value.Range.newBuilder()
                                                                           .setBegin(begin2)
                                                                           .setEnd(end2)
                                                                           .build())
                                               .build()
                          )
                          .setName(name)
                          .setRole(role)
                          .build();
  }

  public static List<Protos.Resource> buildRangeResourceList(int begin, int end) {
    List<Protos.Resource> resourceList = new ArrayList<>();
    resourceList.addAll(
      Arrays.asList(
        buildRangeResource("ports", begin, end),
        buildRangeResourceWithRole("ports", begin, end, "reserved"),
        buildScalarResource("cpus", 1),
        buildScalarResource("mem", 2),
        buildScalarResourceWithRole("cpus", 3, "reserved"),
        buildScalarResourceWithRole("mem", 4, "reserved")
      )
    );
    return resourceList;
  }

  public static Protos.Offer buildOffer() {
    List <Protos.Resource> resourceList = Arrays.asList(
      Protos.Resource
        .newBuilder().setRole("*").setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(1).build()).build(),
      Protos.Resource
        .newBuilder().setRole("*").setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(1).build()).build(),
      Protos.Resource
        .newBuilder().setRole("role").setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(1).build()).build(),
      Protos.Resource
        .newBuilder().setRole("role").setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(Protos.Value.Scalar.newBuilder().setValue(1).build()).build(),
      Protos.Resource.newBuilder().setRole("otherRole").setName("cpus").setType(Protos.Value.Type.SCALAR).setScalar(
        Protos.Value.Scalar.newBuilder().setValue(1).build()).build(),
      Protos.Resource.newBuilder().setRole("otherRole").setName("mem").setType(Protos.Value.Type.SCALAR).setScalar(
        Protos.Value.Scalar.newBuilder().setValue(1).build()).build()
    );
    Collections.shuffle(resourceList);

    return Protos.Offer.newBuilder()
                       .setId(Protos.OfferID.newBuilder().setValue("derp").build())
                       .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("derp").build())
                       .setSlaveId(Protos.SlaveID.newBuilder().setValue("derp").build())
                       .setHostname("derp")
                       .addAllResources(resourceList)
                       .build();
  }

  private static double calculateAllAvailableScalarResources(List<ResourceEntry> resourceEntries) {
    Double retVal = 0.0;
    for (ResourceEntry resourceEntry : resourceEntries) {
      retVal += ((ResourceEntries.ScalarResourceEntry) resourceEntry).getValue();
    }
    return retVal;
  }

  public static List<Long> calculateAllAvailableRangeResources(List<ResourceEntry> resourceEntries) {
    List<Long> retVal = new ArrayList<>();
    for (ResourceEntry resourceEntry : resourceEntries) {
      Long begin = ((ResourceEntries.RangeResourceEntry) resourceEntry).getBegin();
      Long end = ((ResourceEntries.RangeResourceEntry) resourceEntry).getEnd();
      for (long i = begin; i <= end; i++) {
        retVal.add(i);
      }
    }
    return retVal;
  }

  public static double calculateAllAvailableScalarResources(AggregatedOffers aggregatedOffers, ResourceType resourceType) {
    return calculateAllAvailableScalarResources(aggregatedOffers.getAllAvailableResources(resourceType));
  }

  public static List<Long> calculateAllAvailableRangeResources(AggregatedOffers aggregatedOffers, ResourceType resourceType) {
    return calculateAllAvailableRangeResources(aggregatedOffers.getAllAvailableResources(resourceType));
  }

  public static double calculateAllAvailableScalarResources(AggregatedOffers aggregatedOffers, ResourceType resourceType, ReservationType reservationType) {
    return calculateAllAvailableScalarResources(aggregatedOffers.getAllAvailableResources(resourceType, reservationType));
  }

  public static List<Long> calculateAllAvailableRangeResources(AggregatedOffers aggregatedOffers, ResourceType resourceType, ReservationType reservationType) {
    return calculateAllAvailableRangeResources(aggregatedOffers.getAllAvailableResources(resourceType, reservationType));
  }

}
