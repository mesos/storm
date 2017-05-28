/**
 * AndAllConstraintBuilder.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.base.Optional;

import java.util.List;
import java.util.Map;

/**
 *
 * @author fuji-151a
 */
public class AndAllConstraintBuilder<T> implements ConstraintBuilder<T> {

    private final MultiConstraintBuilder<T> builder;

    public AndAllConstraintBuilder(final MultiConstraintBuilder<T> multiConstraintBuilder) {
        this.builder = multiConstraintBuilder;
    }

    @Override
    public Optional<Constraint<T>> build(final Map conf) {
        List<Constraint<T>> ruleList = builder.build(conf);
        if (ruleList.isEmpty()) {
            return Optional.absent();
        }
        Constraint<T> one = ruleList.remove(0);
        if (ruleList.isEmpty()) {
            return Optional.of(one);
        }
        Constraint<T> two = ruleList.remove(0);
        return Optional.of(compose(one, two, ruleList));
    }

    private Constraint<T> compose(final Constraint<T> one,
                                  final Constraint<T> two,
                                  final List<Constraint<T>> tail) {
        Constraint<T> results = new AndConstraint<>(one, two);
        if (tail.isEmpty()) {
            return results;
        }
        Constraint<T> three = tail.remove(0);
        return compose(results, three, tail);
    }
}
