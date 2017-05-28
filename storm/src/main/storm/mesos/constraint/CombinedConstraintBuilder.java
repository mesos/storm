/**
 * CombinedConstraintBuilder.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 *
 * @author fuji-151a
 */
public class CombinedConstraintBuilder<T> implements MultiConstraintBuilder<T> {

    private final List<ConstraintBuilder<T>> list;

    public CombinedConstraintBuilder(ConstraintBuilder<T>... builders) {
        Preconditions.checkArgument(builders.length > 0, "builders should not be empty!!");
        this.list = ImmutableList.copyOf(builders);
    }

    @Override
    public List<Constraint<T>> build(Map conf) {
        List<Constraint<T>> result = Lists.newArrayList();
        for (ConstraintBuilder<T> builder : list) {
            Optional<Constraint<T>> constraint = builder.build(conf);
            if (constraint.isPresent()) {
                result.add(constraint.get());
            }
        }
        return result;
    }
}
