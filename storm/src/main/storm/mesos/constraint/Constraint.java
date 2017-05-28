/**
 * Constraint.java
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
public interface Constraint<T> {
    boolean isAccepted(T target);
}
