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
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.junit.Test;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

// TODO(dskarthick) : Leverage the build methods defined in TestUtils function.
public class MesosNimbusTest {

  @Test
  public void testGetResourcesScalar() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(TestUtils.buildScalarResource("cpus", 1.0)),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            1.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(TestUtils.buildScalarResource("mem", 2.0)),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            2.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("cpus", 1.0),
            TestUtils.buildScalarResourceWithRole("cpus", 1.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            2.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("mem", 1.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            3.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("cpus", 1.0),
            TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            4.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            6.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("cpus", 1.0),
            TestUtils.buildScalarResourceWithRole("cpus", 0.5, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            1.5,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("mem", 0.5, "reserved")
        ),
        mesosNimbus.getResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
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
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            1.0,
            "cpus"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("cpus", 1.0),
            TestUtils.buildScalarResource("mem", 1.0),
            TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            1.0,
            "mem"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("cpus", 2.5, "reserved"),
            TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesScalar(
            TestUtils.buildResourceList(1, 2, 3, 4),
            1.5,
            "cpus"
        )
    );

    assertEquals(
      Arrays.asList(
        TestUtils.buildScalarResource("cpus", 1.0),
        TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved"),
        TestUtils.buildScalarResourceWithRole("mem", 3.5, "reserved")
      ),
      mesosNimbus.subtractResourcesScalar(
        TestUtils.buildResourceList(1, 2, 3, 4),
        2.5,
        "mem"
      )
    );
  }

  @Test
  public void testGetResourcesRange() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

    assertEquals(
        Arrays.asList(
            TestUtils.buildRangeResource("ports", 100, 100)
        ),
        mesosNimbus.getResourcesRange(
            TestUtils.buildRangeResourceList(100, 100),
            100,
            "ports"
        )
    );

    assertEquals(
        Arrays.asList(
            TestUtils.buildRangeResource("ports", 105, 105)
        ),
        mesosNimbus.getResourcesRange(
            TestUtils.buildRangeResourceList(100, 200),
            105,
            "ports"
        )
    );

    assertEquals(
        0,
        mesosNimbus.getResourcesRange(
            TestUtils.buildRangeResourceList(100, 100),
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
            TestUtils.buildScalarResource("cpus", 1.0),
            TestUtils.buildScalarResource("mem", 2.0),
            TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved"),
            TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
        ),
        mesosNimbus.subtractResourcesRange(
            TestUtils.buildRangeResourceList(100, 100),
            100,
            "ports"
        )
    );

    assertEquals(
      Arrays.asList(
        TestUtils.buildMultiRangeResource("ports", 100, 104, 106, 200),
        TestUtils.buildMultiRangeResourceWithRole("ports", 100, 104, 106, 200, "reserved"),
        TestUtils.buildScalarResource("cpus", 1.0),
        TestUtils.buildScalarResource("mem", 2.0),
        TestUtils.buildScalarResourceWithRole("cpus", 3.0, "reserved"),
        TestUtils.buildScalarResourceWithRole("mem", 4.0, "reserved")
      ),
      mesosNimbus.subtractResourcesRange(
        TestUtils.buildRangeResourceList(100, 200),
        105,
        "ports"
      )
    );
  }


  @Test
  public void testComputeResourcesForSlot() throws Exception {
    MesosNimbus mesosNimbus = new MesosNimbus();

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
        TestUtils.buildOfferWithPorts("offer1", "host1.west", 2.0, 2048, 1000, 1000)
    );

    HashMap<String, TopologyDetails> topologyMap = new HashMap<>();
    Map conf = new HashMap<>();
    conf.put(MesosCommon.WORKER_CPU_CONF, 1);
    conf.put(MesosCommon.WORKER_MEM_CONF, 1024);
    conf.put(MesosCommon.EXECUTOR_CPU_CONF, 1);
    conf.put(MesosCommon.EXECUTOR_MEM_CONF, 1024);
    conf.put(MesosNimbus.CONF_EXECUTOR_URI, "");
    mesosNimbus._conf = conf;

    topologyMap.put("t1", new TopologyDetails("t1", conf, new StormTopology(), 5));
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
        TestUtils.buildScalarResource("cpus", 1.0),
        launchList.get(offerId).get(0).getTask().getResources(0)
    );

    assertEquals(0, offers.get(offerId).getResourcesCount());
  }
}
