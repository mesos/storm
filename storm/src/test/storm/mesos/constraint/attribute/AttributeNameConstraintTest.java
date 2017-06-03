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
package storm.mesos.constraint.attribute;

import org.apache.mesos.Protos.Attribute;
import org.junit.Test;
import storm.mesos.TestUtils;
import storm.mesos.constraint.Constraint;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author fuji-151a
 */
public class AttributeNameConstraintTest {

  @Test
  public void isAccepted() throws Exception {
    Attribute attr = TestUtils.buildTextAttribute("host", "test");
    Constraint<Attribute> constraintAttribute = new AttributeNameConstraint("host");
    assertTrue(constraintAttribute.isAccepted(attr));
  }

  @Test
  public void isNotAccepted() throws Exception {
    Attribute attr = TestUtils.buildTextAttribute("ip", "test");
    Constraint<Attribute> constraintAttribute = new AttributeNameConstraint("host");
    assertFalse(constraintAttribute.isAccepted(attr));
  }
}
