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
package storm.mesos.constraint.offer;

import com.google.common.base.Optional;
import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;
import storm.mesos.constraint.Constraint;
import storm.mesos.constraint.ConstraintBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author fuji-151a
 */
public class HostEqOfferConstraintTest {

  @Test
  public void isAcceptedAllowedHosts() throws Exception {
    ConstraintBuilder<Offer> allowedHostsBuilder = new HostEqOfferConstraint.AllowedHostsBuilder();
    Optional<Constraint<Offer>> offerConstraint
        = allowedHostsBuilder.build(
        createHostsConf("mesos.allowed.hosts")
    );
    assertTrue(offerConstraint.get().isAccepted(TestUtils.buildOffer("id", "hostA", 1, 1)));
  }

  @Test
  public void isNotAcceptedAllowedHosts() throws Exception {
    ConstraintBuilder<Offer> allowedHostsBuilder = new HostEqOfferConstraint.AllowedHostsBuilder();
    Optional<Constraint<Offer>> offerConstraint
        = allowedHostsBuilder.build(
        createHostsConf("mesos.allowed.hosts")
    );
    assertFalse(offerConstraint.get().isAccepted(TestUtils.buildOffer("id", "hostC", 1, 1)));
  }

  @Test
  public void isAcceptedDisallowedHosts() throws Exception {
    ConstraintBuilder<Offer> allowedHostsBuilder = new HostEqOfferConstraint.DisallowedHostsBuilder();
    Optional<Constraint<Offer>> offerConstraint
        = allowedHostsBuilder.build(
        createHostsConf("mesos.disallowed.hosts")
    );
    assertFalse(offerConstraint.get().isAccepted(TestUtils.buildOffer("id", "hostA", 1, 1)));
  }

  @Test
  public void isNotAcceptedDisallowedHosts() throws Exception {
    ConstraintBuilder<Offer> allowedHostsBuilder = new HostEqOfferConstraint.DisallowedHostsBuilder();
    Optional<Constraint<Offer>> offerConstraint
        = allowedHostsBuilder.build(
        createHostsConf("mesos.disallowed.hosts")
    );
    assertTrue(offerConstraint.get().isAccepted(TestUtils.buildOffer("id", "hostC", 1, 1)));
  }

  private Map<String, List<String>> createHostsConf(String key) {
    List<String> hostList = new ArrayList<>();
    hostList.add("hostA");
    hostList.add("hostB");
    Map<String, List<String>> stormConf = new HashMap<>();
    stormConf.put(key, hostList);
    return stormConf;
  }
}
