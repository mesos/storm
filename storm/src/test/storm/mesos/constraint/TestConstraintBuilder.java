/**
 * TestConstraintBuilder.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.base.Optional;
import org.apache.mesos.Protos.Offer;

import java.util.Map;

/**
 *
 * @author fuji-151a
 */
public class TestConstraintBuilder implements ConstraintBuilder<Offer> {

    @Override
    public Optional<Constraint<Offer>> build(Map conf) {
        Constraint<Offer> offer = new TestConstraint<Offer>(true);
        return Optional.of(offer);
    }
}
