/**
 * AndConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

/**
 * @author fuji-151a
 */
public class AndConstraint<T> implements Constraint<T> {
  private final Constraint<T> one;
  private final Constraint<T> two;

  public AndConstraint(final Constraint<T> a, final Constraint<T> b) {
    this.one = a;
    this.two = b;
  }

  @Override
  public boolean isAccepted(final T target) {
    return one.isAccepted(target) && two.isAccepted(target);
  }
}
