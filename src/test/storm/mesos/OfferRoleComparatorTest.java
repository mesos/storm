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

import static org.junit.Assert.*;
import org.apache.mesos.Protos.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class OfferRoleComparatorTest {

  public Offer buildOffer() {
    List <Resource> resourceList = Arrays.asList(
                Resource.newBuilder().setRole("*").setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build(),
                Resource.newBuilder().setRole("*").setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build(),
                Resource.newBuilder().setRole("role").setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build(),
                Resource.newBuilder().setRole("role").setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build(),
                Resource.newBuilder().setRole("otherRole").setName("cpus").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build(),
                Resource.newBuilder().setRole("otherRole").setName("mem").setType(Value.Type.SCALAR).setScalar(Value.Scalar.newBuilder().setValue(1).build()).build()
            );
    Collections.shuffle(resourceList);

    return Offer.newBuilder()
        .setId(OfferID.newBuilder().setValue("derp").build())
        .setFrameworkId(FrameworkID.newBuilder().setValue("derp").build())
        .setSlaveId(SlaveID.newBuilder().setValue("derp").build())
        .setHostname("derp")
        .addAllResources(resourceList)
        .build();
  }

  @Test
  public void testCompare() throws Exception {
    List<Resource> offerResources = new ArrayList<>();
    Offer offer = buildOffer();
    offerResources.addAll(offer.getResourcesList());
    Collections.sort(offerResources, new ResourceRoleComparator());

    assertEquals(
        "*",
        offerResources.get(5).getRole()
    );
    assertEquals(
        "*",
        offerResources.get(4).getRole()
    );
  }
}
