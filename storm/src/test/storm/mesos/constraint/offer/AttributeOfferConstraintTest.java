/**
 * AttributeOfferConstraintTest.java
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
import storm.mesos.constraint.TestMultiConstraintBuilder;
import storm.mesos.constraint.attribute.AttributeNameConstraint;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author fuji-151a
 */
public class AttributeOfferConstraintTest {

    @Test
    public void isAccepted() {
        ConstraintBuilder<Offer> constraintOffer
                = new AttributeOfferConstraint.Builder(
                        new TestMultiConstraintBuilder<>(
                                new AttributeNameConstraint("type")
                        )
        );
        Optional<Constraint<Offer>> constraintOptional = constraintOffer.build(new HashMap());
        assertTrue(constraintOptional.get().isAccepted(TestUtils.buildOfferWithTextAttributes("test","hostA", "type", "server")));
    }

    @Test
    public void isNotAccepted() {
        ConstraintBuilder<Offer> constraintOffer
                = new AttributeOfferConstraint.Builder(
                new TestMultiConstraintBuilder<>(
                        new AttributeNameConstraint("dummy")
                )
        );
        Optional<Constraint<Offer>> constraintOptional = constraintOffer.build(new HashMap());
        assertFalse(constraintOptional.get().isAccepted(TestUtils.buildOfferWithTextAttributes("test","hostA", "type", "server")));
    }
}