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

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author fuji-151a
 */
public class CombinedConstraintBuilderTest {
  @Test
  public void build() throws Exception {
    MultiConstraintBuilder<Offer> multiCB = new CombinedConstraintBuilder<>(
        new TestConstraintBuilder(),
        new TestConstraintBuilder()
    );
    List<Constraint<Offer>> constraintOfferList = multiCB.build(new HashMap());
    assertThat(constraintOfferList.size(), is(2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void illegalArgumentExceptionTest() {
    MultiConstraintBuilder<Offer> multiCB = new CombinedConstraintBuilder<>();
    multiCB.build(new HashMap());
  }

}
