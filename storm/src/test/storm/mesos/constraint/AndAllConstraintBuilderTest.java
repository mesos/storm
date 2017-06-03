/**
 * AndAllConstraintBuilderTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.base.Optional;
import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author fuji-151a
 */
public class AndAllConstraintBuilderTest {

  @Test
  public void build0Value() {
    ConstraintBuilder<Offer> constraintBuilder =
        new AndAllConstraintBuilder<>(
            new TestMultiConstraintBuilder<Offer>()
        );
    Optional<Constraint<Offer>> constraintOptional
        = constraintBuilder.build(new HashMap());
    assertNull(constraintOptional.orNull());

  }

  @Test
  public void build1Value() {
    ConstraintBuilder<Offer> constraintBuilder =
        new AndAllConstraintBuilder<>(
            new TestMultiConstraintBuilder<Offer>(
                new TestConstraint(true)
            )
        );
    Optional<Constraint<Offer>> constraintOptinal
        = constraintBuilder.build(new HashMap());
    assertTrue(constraintOptinal.get().isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void build2Value() {
    ConstraintBuilder<Offer> constraintBuilder =
        new AndAllConstraintBuilder<>(
            new TestMultiConstraintBuilder<Offer>(
                new TestConstraint(true),
                new TestConstraint(false)
            )
        );
    Optional<Constraint<Offer>> constraintOptional
        = constraintBuilder.build(new HashMap());
    assertFalse(constraintOptional.get().isAccepted(TestUtils.buildOffer()));
  }
}
