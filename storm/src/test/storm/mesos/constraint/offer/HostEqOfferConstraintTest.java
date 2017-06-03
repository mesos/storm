/**
 * HostEqOfferConstraintTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
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
