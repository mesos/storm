/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 *
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
