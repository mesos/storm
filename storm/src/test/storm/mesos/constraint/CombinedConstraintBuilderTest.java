/**
 * CombinedConstraintBuilderTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import org.apache.mesos.Protos.Offer;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author fuji-151a
 */
public class CombinedConstraintBuilderTest {
  @Test
  public void build() throws Exception {
    MultiConstraintBuilder<Offer> multiCB = new CombinedConstraintBuilder<>(
        new TestConstraintBuilder(),
        new TestConstraintBuilder()
    );
    List<Constraint<Offer>> constraintOfferList = multiCB.build(new HashMap());
    assertThat(constraintOfferList.size(), is(2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentExceptionTest() {
    MultiConstraintBuilder<Offer> multiCB = new CombinedConstraintBuilder<>();
    multiCB.build(new HashMap());
  }

}