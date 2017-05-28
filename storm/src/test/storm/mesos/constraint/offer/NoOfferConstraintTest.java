/**
 * NoOfferConstraintTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.offer;

import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;
import storm.mesos.constraint.Constraint;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author fuji-151a
 */
public class NoOfferConstraintTest {

    @Test
    public void isAccepted() throws Exception {
        Constraint<Offer> offerConstraint = new NoOfferConstraint();
        assertTrue(offerConstraint.isAccepted(TestUtils.buildOffer()));
    }

}