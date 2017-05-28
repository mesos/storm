/**
 * TestConstraint.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;

/**
 *
 * @author fuji-151a
 */
public class TestConstraint implements Constraint<Offer> {

    private final boolean b;

    public TestConstraint(boolean bool) {
        this.b = bool;
    }

    @Override
    public boolean isAccepted(Offer target) {
        return b;
    }
}
