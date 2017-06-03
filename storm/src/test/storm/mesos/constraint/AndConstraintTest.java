/**
 * AndConstraintTest.java
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
public class AndConstraintTest {

  @Test
  public void trueAndTrueIsAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(true);
    Constraint<Offer> b = new TestConstraint(true);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertTrue(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void trueAndFalseIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(true);
    Constraint<Offer> b = new TestConstraint(false);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void falseAndTrueIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(false);
    Constraint<Offer> b = new TestConstraint(true);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void falseAndFalseIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(false);
    Constraint<Offer> b = new TestConstraint(false);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

}
