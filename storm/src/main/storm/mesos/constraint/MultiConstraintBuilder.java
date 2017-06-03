/**
 * MultiConstraintBuilder.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import java.util.List;
import java.util.Map;

/**
 * @author fuji-151a
 */
public interface MultiConstraintBuilder<T> {
  List<Constraint<T>> build(Map conf);
}
