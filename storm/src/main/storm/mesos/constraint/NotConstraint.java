/**
 * NotConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

/**
 *
 * @author fuji-151a
 */
public class NotConstraint<T> implements Constraint<T> {
    private final Constraint<T> constraint;
    public NotConstraint(final Constraint<T> rule) {
        this.constraint = rule;
    }
    @Override
    public boolean isAccepted(final T target) {
        return !constraint.isAccepted(target);
    }
}
