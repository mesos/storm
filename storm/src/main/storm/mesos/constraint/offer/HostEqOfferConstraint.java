/**
 * HostEqOfferConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.offer;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import storm.mesos.constraint.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author fuji-151a
 */
public class HostEqOfferConstraint implements Constraint<Offer> {

  public static final ConstraintBuilder<Offer> DEFAULT_BUILDER =
      new AndAllConstraintBuilder<>(new CombinedConstraintBuilder<>(
          new AllowedHostsBuilder(), new DisallowedHostsBuilder()));

  private final Set<String> hosts;

  public HostEqOfferConstraint(final Set<String> hostSet) {
    this.hosts = hostSet;
  }

  @Override
  public boolean isAccepted(final Offer target) {
    return target.hasHostname() && hosts.contains(target.getHostname());
  }

  public static class AllowedHostsBuilder implements ConstraintBuilder<Offer> {
    private static final String STORM_CONF_KEY = "mesos.allowed.hosts";

    @Override
    public Optional<Constraint<Offer>> build(final Map conf) {
      Object obj = conf.get(STORM_CONF_KEY);
      if (obj == null) {
        return Optional.absent();
      }
      Set<String> hostSet = listToSet(obj, STORM_CONF_KEY);
      Constraint<Offer> constraint = new HostEqOfferConstraint(hostSet);
      return Optional.of(constraint);
    }
  }

  public static class DisallowedHostsBuilder implements ConstraintBuilder<Offer> {
    private static final String STORM_CONF_KEY = "mesos.disallowed.hosts";

    @Override
    public Optional<Constraint<Offer>> build(final Map conf) {
      Object obj = conf.get(STORM_CONF_KEY);
      if (obj == null) {
        return Optional.absent();
      }
      Set<String> hostSet = listToSet(obj, STORM_CONF_KEY);
      Constraint<Offer> constraint = new NotConstraint<>(new HostEqOfferConstraint(hostSet));
      return Optional.of(constraint);
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<String> listToSet(Object obj, String key) {
    Preconditions.checkArgument(obj instanceof List, key + " should be List.");
    List<String> hostList = (List<String>) obj;
    Preconditions.checkArgument(!hostList.isEmpty(), key + " should not be empty!!");
    return ImmutableSet.copyOf(hostList);
  }
}
