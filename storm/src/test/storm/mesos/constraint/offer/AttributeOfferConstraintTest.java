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

import com.google.common.base.Optional;
import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;
import storm.mesos.constraint.Constraint;
import storm.mesos.constraint.ConstraintBuilder;
import storm.mesos.constraint.TestMultiConstraintBuilder;
import storm.mesos.constraint.attribute.AttributeNameConstraint;

import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author fuji-151a
 */
public class AttributeOfferConstraintTest {

  @Test
  public void isAccepted() {
    ConstraintBuilder<Offer> constraintOffer
        = new AttributeOfferConstraint.Builder(
        new TestMultiConstraintBuilder<>(
            new AttributeNameConstraint("type")
        )
    );
    Optional<Constraint<Offer>> constraintOptional = constraintOffer.build(new HashMap());
    assertTrue(constraintOptional.get().isAccepted(TestUtils.buildOfferWithTextAttributes("test", "hostA", "type", "server")));
  }

  @Test
  public void isNotAccepted() {
    ConstraintBuilder<Offer> constraintOffer
        = new AttributeOfferConstraint.Builder(
        new TestMultiConstraintBuilder<>(
            new AttributeNameConstraint("dummy")
        )
    );
    Optional<Constraint<Offer>> constraintOptional = constraintOffer.build(new HashMap());
    assertFalse(constraintOptional.get().isAccepted(TestUtils.buildOfferWithTextAttributes("test", "hostA", "type", "server")));
  }
}