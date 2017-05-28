/**
 * AttributeNameConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.attribute;

import org.apache.mesos.Protos.Attribute;
import storm.mesos.constraint.Constraint;

/**
 *
 * @author fuji-151a
 */
public class AttributeNameConstraint implements Constraint<Attribute> {
    private final String name;
    public AttributeNameConstraint(final String key) {
        this.name = key;
    }
    @Override
    public boolean isAccepted(final Attribute target) {
        return target.hasName() && name.equals(target.getName());
    }
}
