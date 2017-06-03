/**
 * ConstraintBuilder.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.base.Optional;

import java.util.Map;

/**
 * @author fuji-151a
 */
public interface ConstraintBuilder<T> {
  Optional<Constraint<T>> build(Map conf);
}
