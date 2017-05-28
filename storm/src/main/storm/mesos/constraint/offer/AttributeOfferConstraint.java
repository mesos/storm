/**
 * AttributeOfferConstraint.java
 *
 * @since 2017/05/20
 * <p/>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.offer;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.Offer;
import storm.mesos.constraint.AndAllConstraintBuilder;
import storm.mesos.constraint.Constraint;
import storm.mesos.constraint.ConstraintBuilder;
import storm.mesos.constraint.MultiConstraintBuilder;

import java.util.Map;

/**
 * This class receives offer and check constraint on attributes.
 * @author fuji-151a
 */
public class AttributeOfferConstraint implements Constraint<Offer> {
    private final Constraint<Attribute> constraint;

    public AttributeOfferConstraint(final Constraint<Attribute> attribute) {
        this.constraint = attribute;
    }

    @Override
    public boolean isAccepted(final Offer target) {
        for (Attribute offerAttr : target.getAttributesList()) {
            if (constraint.isAccepted(offerAttr)) {
                return true;
            }
        }
        return false;
    }

    public static class Builder implements ConstraintBuilder<Offer> {

        private final ConstraintBuilder<Attribute> constraintBuilder;

        public Builder(final MultiConstraintBuilder<Attribute> multiBuilder) {
            this.constraintBuilder = new AndAllConstraintBuilder<>(multiBuilder);
        }

        @Override
        public Optional<Constraint<Offer>> build(final Map conf) {
            Optional<Constraint<Attribute>> result = constraintBuilder.build(conf);
            return result.transform(new Function<Constraint<Attribute>, Constraint<Offer>>() {
                @Override
                public Constraint<Offer> apply(Constraint<Attribute> attributeConstraint) {
                    return new AttributeOfferConstraint(attributeConstraint);
                }
            });
        }
    }
}
