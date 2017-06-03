/**
 * TestMultiConstraintBuilder.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author fuji-151a
 */
public class TestMultiConstraintBuilder<T> implements MultiConstraintBuilder<T> {

  private final List<Constraint<T>> constraintList;

  public TestMultiConstraintBuilder() {
    constraintList = new ArrayList<>();
  }

  public TestMultiConstraintBuilder(Constraint<T>... constraint) {
    this.constraintList = Arrays.asList(constraint);
  }

  @Override
  public List<Constraint<T>> build(Map conf) {
    List<Constraint<T>> returnList = new ArrayList<>();
    returnList.addAll(constraintList);
    return returnList;
  }
}
