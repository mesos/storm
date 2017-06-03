/**
 * NoOfferConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.offer;

import org.apache.mesos.Protos.Offer;
import storm.mesos.constraint.Constraint;

/**
 * @author fuji-151a
 */
public class NoOfferConstraint implements Constraint<Offer> {
  @Override
  public boolean isAccepted(final Offer target) {
    return true;
  }
}
