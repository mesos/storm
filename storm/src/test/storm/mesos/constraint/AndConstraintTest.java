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
package storm.mesos.constraint;

import org.apache.mesos.Protos.Offer;
import org.junit.Test;
import storm.mesos.TestUtils;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * @author fuji-151a
 */
public class AndConstraintTest {

  @Test
  public void trueAndTrueIsAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(true);
    Constraint<Offer> b = new TestConstraint(true);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertTrue(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void trueAndFalseIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(true);
    Constraint<Offer> b = new TestConstraint(false);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void falseAndTrueIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(false);
    Constraint<Offer> b = new TestConstraint(true);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

  @Test
  public void falseAndFalseIsNotAccepted() throws Exception {
    Constraint<Offer> a = new TestConstraint(false);
    Constraint<Offer> b = new TestConstraint(false);
    Constraint<Offer> offerConstraint = new AndConstraint<>(a, b);
    assertFalse(offerConstraint.isAccepted(TestUtils.buildOffer()));
  }

}