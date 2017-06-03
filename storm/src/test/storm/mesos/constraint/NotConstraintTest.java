/**
 * NotConstraintTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * @author fuji-151a
 */
public class NotConstraintTest {

  @Test
  public void isAccepted() throws Exception {
    Constraint<Offer> testOfferConstraint = new TestConstraint(false);
    Constraint<Offer> offerConstraint = new NotConstraint<>(testOfferConstraint);
    assertTrue(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void isNotAccepted() throws Exception {
    Constraint<Offer> testOfferConstraint = new TestConstraint(true);
    Constraint<Offer> offerConstraint = new NotConstraint<>(testOfferConstraint);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }
}
