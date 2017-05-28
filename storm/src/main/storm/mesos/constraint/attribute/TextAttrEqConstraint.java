/**
 * TextAttrEqConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.attribute;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.storm.shade.com.google.common.base.Preconditions;
import storm.mesos.constraint.AndConstraint;
import storm.mesos.constraint.Constraint;
import storm.mesos.constraint.MultiConstraintBuilder;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 *
 * @author fuji-151a
 */
public class TextAttrEqConstraint implements Constraint<Attribute> {

    private final String value;
    public TextAttrEqConstraint(final String text) {
        this.value = text;
    }

    @Override
    public boolean isAccepted(final Attribute target) {
        return target.hasText() && value.equals(target.getText().getValue());
    }

    public static class Builder implements MultiConstraintBuilder<Attribute> {
        private static final String STORM_CONF_KEY = "mesos.constraint";

        @Override
        public List<Constraint<Attribute>> build(final Map conf) {
            Object obj = conf.get(STORM_CONF_KEY);
            List<Constraint<Attribute>> list = new LinkedList<>();
            if (obj == null) {
                return list;
            }
            Map<String, String> map = objToMap(obj);
            for (Map.Entry<String, String> entry : map.entrySet()) {
                Constraint<Attribute> key = new AttributeNameConstraint(entry.getKey());
                Constraint<Attribute> val = new TextAttrEqConstraint(entry.getValue());
                list.add(new AndConstraint<>(key, val));
            }
            return list;
        }

        @SuppressWarnings("unchecked")
        private Map<String, String> objToMap(Object obj) {
            Preconditions.checkArgument(obj instanceof Map, STORM_CONF_KEY + " should be Map.");
            Map<String, String> map = (Map<String, String>) obj;
            Preconditions.checkArgument(!map.isEmpty(), STORM_CONF_KEY + " should not be empty!!");
            return map;
        }
    }
}
