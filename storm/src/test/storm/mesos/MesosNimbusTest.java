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

import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MesosNimbusTest {

  private Offer buildOffer(double cpus, double mem) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue("derp").build())
        .setFrameworkId(FrameworkID.newBuilder().setValue("derp").build())
        .setSlaveId(SlaveID.newBuilder().setValue("derp").build())
        .setHostname("derp")
        .addAllResources(
            Arrays.asList(
                buildScalarResource("cpus", cpus),
                buildScalarResourceWithReservation("cpus", 1.0, "dynamicallyReserved"),
                buildScalarResource("mem", mem)
            )
        )
        .build();
  }

  private Offer buildOfferWithPorts(double cpus, double mem, int portBegin, int portEnd) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue("derp").build())
        .setFrameworkId(FrameworkID.newBuilder().setValue("derp").build())
        .setSlaveId(SlaveID.newBuilder().setValue("derp").build())
        .setHostname("derp")
        .addAllResources(
            Arrays.asList(
                buildScalarResource("cpus", cpus),
                buildScalarResourceWithReservation("cpus", 1.0, "dynamicallyReserved"),
                buildScalarResource("mem", mem),
                buildRangeResource("ports", portBegin, portEnd)
            )
        )
        .build();
  }

  private Offer buildOfferWithReservation(double cpus, double mem, double reservedCpu, double reservedMem) {
    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue("derp").build())
        .setFrameworkId(FrameworkID.newBuilder().setValue("derp").build())
        .setSlaveId(SlaveID.newBuilder().setValue("derp").build())
        .setHostname("derp")
        .addAllResources(
            Arrays.asList(
                buildScalarResource("cpus", cpus),
                buildScalarResourceWithReservation("cpus", 1.0, "dynamicallyReserved"),
                buildScalarResource("mem", mem),
                buildScalarResourceWithRole("cpus", reservedCpu, "reserved"),
                buildScalarResourceWithRole("mem", reservedMem, "reserved")
            )
        )
        .build();
  }

  @Test
  public void testGetResources() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    Offer offer1 = buildOffer(2.0, 2.0);
    OfferResources offerResources1 = mesosNimbus.getResources(offer1, 1.0, 1.0, 1.0, 1.0);
    assertEquals(1, offerResources1.cpuSlots);
    assertEquals(1, offerResources1.memSlots);

    Offer offer2 = buildOffer(1.0, 1.0);
    OfferResources offerResources2 = mesosNimbus.getResources(offer2, 1.0, 1.0, 1.0, 1.0);
    assertEquals(0, offerResources2.cpuSlots);
    assertEquals(0, offerResources2.memSlots);

    Offer offer3 = buildOfferWithReservation(2.0, 2.0, 1.0, 1.0);
    OfferResources offerResources3 = mesosNimbus.getResources(offer3, 1.0, 1.0, 1.0, 1.0);
    assertEquals(2, offerResources3.cpuSlots);
    assertEquals(2, offerResources3.memSlots);

    Offer offer4 = buildOfferWithReservation(2.0, 2.0, 1.5, 1.5);
    OfferResources offerResources4 = mesosNimbus.getResources(offer4, 2.5, 2.5, 1.0, 1.0);
    assertEquals(1, offerResources4.cpuSlots);
    assertEquals(1, offerResources4.memSlots);
  }

  @Test
  public void testGetResourcesScalar() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(buildScalarResource("cpus", 1.0)),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            1.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(buildScalarResource("mem", 2.0)),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            2.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResourceWithRole("cpus", 1.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            2.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("mem", 1.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            3.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            4.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            6.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResourceWithRole("cpus", 0.5, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            1.5,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("mem", 0.5, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            2.5,
            "mem"
        )
    );
  }

  @Test
  public void testSubtractResourcesScalar() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            1.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResource("mem", 1.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            1.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("cpus", 2.5, "reserved"),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            1.5,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            buildScalarResourceWithRole("mem", 3.5, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            buildResourceList(1, 2, 3, 4),
            2.5,
            "mem"
        )
    );
  }

  private Resource buildScalarResource(String name, double value) {
    return Resource.newBuilder()
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
            .setValue(value)
            .build())
        .setName(name)
        .build();
  }

  private Resource buildScalarResourceWithRole(String name, double value, String role) {
    return Resource.newBuilder()
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
            .setValue(value)
            .build())
        .setName(name)
        .setRole(role)
        .build();
  }

  private Resource buildScalarResourceWithReservation(String name, double value, String role) {
    return Resource.newBuilder()
        .setType(Value.Type.SCALAR)
        .setScalar(Value.Scalar.newBuilder()
            .setValue(value)
            .build())
        .setName(name)
        .setRole(role)
        .setReservation(
            Resource.ReservationInfo.newBuilder()
                .setPrincipal("derp")
                .build()
        )
        .build();
  }

  private List<Resource> buildResourceList(double cpus, double mem, double reservedCpu, double reservedMem) {
    List<Resource> resourceList = new ArrayList<>();
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

  @Test
  public void testGetResourcesRange() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(
            buildRangeResource("ports", 100, 100)
        ),
        mesosNimbus.getResourcesRange(
            buildRangeResourceList(100, 100),
            100,
            "ports"
        )
    );

    assertEquals(
        Arrays.asList(
            buildRangeResource("ports", 105, 105)
        ),
        mesosNimbus.getResourcesRange(
            buildRangeResourceList(100, 200),
            105,
            "ports"
        )
    );

    assertEquals(
        0,
        mesosNimbus.getResourcesRange(
            buildRangeResourceList(100, 100),
            200,
            "ports"
        ).size()
    );
  }


  @Test
  public void testSubtractResourcesRange() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(
            buildScalarResource("cpus", 1.0),
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesRange(
            buildRangeResourceList(100, 100),
            100,
            "ports"
        )
    );

    assertEquals(
        Arrays.asList(
            buildMultiRangeResource("ports", 100, 104, 106, 200),
            buildMultiRangeResourceWithRole("ports", 100, 104, 106, 200, "reserved"),
            buildScalarResource("cpus", 1.0),
            buildScalarResource("mem", 2.0),
            buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesRange(
            buildRangeResourceList(100, 200),
            105,
            "ports"
        )
    );
  }


  private Resource buildRangeResource(String name, int begin, int end) {
    return Resource.newBuilder()
        .setType(Value.Type.RANGES)
        .setRanges(
            Value.Ranges.newBuilder()
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin)
                    .setEnd(end)
                    .build())
                .build()
        )
        .setName(name)
        .build();
  }

  private Resource buildRangeResourceWithRole(String name, int begin, int end, String role) {
    return Resource.newBuilder()
        .setType(Value.Type.RANGES)
        .setRanges(
            Value.Ranges.newBuilder()
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin)
                    .setEnd(end)
                    .build())
                .build()
        )
        .setName(name)
        .setRole(role)
        .build();
  }

  private Resource buildMultiRangeResource(String name, int begin1, int end1, int begin2, int end2) {
    return Resource.newBuilder()
        .setType(Value.Type.RANGES)
        .setRanges(
            Value.Ranges.newBuilder()
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin1)
                    .setEnd(end1)
                    .build())
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin2)
                    .setEnd(end2)
                    .build())
                .build()
        )
        .setName(name)
        .build();
  }

  private Resource buildMultiRangeResourceWithRole(String name, int begin1, int end1, int begin2, int end2, String role) {
    return Resource.newBuilder()
        .setType(Value.Type.RANGES)
        .setRanges(
            Value.Ranges.newBuilder()
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin1)
                    .setEnd(end1)
                    .build())
                .addRange(Value.Range.newBuilder()
                    .setBegin(begin2)
                    .setEnd(end2)
                    .build())
                .build()
        )
        .setName(name)
        .setRole(role)
        .build();
  }

  private List<Resource> buildRangeResourceList(int begin, int end) {
    List<Resource> resourceList = new ArrayList<>();
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

  @Test
  public void testComputeResourcesForSlot() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    mesosNimbus._driver = Mockito.any(MesosSchedulerDriver.class);
    mesosNimbus._configUrl = new URI("http://127.0.0.1/");

    OfferID offerId = OfferID.newBuilder().setValue("derp").build();
    RotatingMap<OfferID, Offer> offers = new RotatingMap<>(
        new RotatingMap.ExpiredCallback<OfferID, Offer>() {
          @Override
          public void expire(OfferID key, Offer val) {
          }
        }
    );

    offers.put(
        offerId,
        buildOfferWithPorts(2.0, 2048, 1000, 1000)
    );

    HashMap<String, TopologyDetails> topologyMap = new HashMap<>();
    Map conf = new HashMap<>();
    conf.put(MesosCommon.WORKER_CPU_CONF, 1);
    conf.put(MesosCommon.WORKER_MEM_CONF, 1024);
    conf.put(MesosCommon.EXECUTOR_CPU_CONF, 1);
    conf.put(MesosCommon.EXECUTOR_MEM_CONF, 1024);
    conf.put(MesosNimbus.CONF_EXECUTOR_URI, "");
    mesosNimbus._conf = conf;

    topologyMap.put("t1", new TopologyDetails("t1", conf, Mockito.any(StormTopology.class), 5));
    HashMap<OfferID, List<LaunchTask>> launchList = new HashMap<>();
    HashMap<OfferID, List<WorkerSlot>> slotList = new HashMap<>();
    slotList.put(offerId, Arrays.asList(new WorkerSlot("", 1000)));
    Topologies topologies = new Topologies(topologyMap);

    mesosNimbus.computeResourcesForSlot(
        offers,
        topologies,
        launchList,
        "t1",
        slotList,
        OfferID.newBuilder().setValue("derp").build()
    );

    assertEquals(1, launchList.size());
    assertEquals(1, launchList.get(offerId).size());

    assertEquals(
        buildScalarResource("cpus", 1.0),
        launchList.get(offerId).get(0).getTask().getResources(0)
    );

    assertEquals(0, offers.get(offerId).getResourcesCount());
  }
}
