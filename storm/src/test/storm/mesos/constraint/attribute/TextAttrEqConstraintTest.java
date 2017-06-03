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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author fuji-151a
 */
public class TextAttrEqConstraintTest {

  @Test
  public void isAccepted() throws Exception {
    Map<String, String> cons = new HashMap<>();
    cons.put("type", "host");
    Map<String, Map> conf = new HashMap<>();
    conf.put("mesos.constraint", cons);
    List<Constraint<Attribute>> constraintList = new TextAttrEqConstraint.Builder().build(conf);
    Attribute attr = TestUtils.buildTextAttribute("type", "host");
    for (Constraint<Attribute> constraint : constraintList) {
      assertTrue(constraint.isAccepted(attr));
    }
  }

  @Test
  public void isNotAccepted() throws Exception {
    Map<String, String> cons = new HashMap<>();
    cons.put("type", "host");
    Map<String, Map> conf = new HashMap<>();
    conf.put("mesos.constraint", cons);
    List<Constraint<Attribute>> constraintList = new TextAttrEqConstraint.Builder().build(conf);
    Attribute attr = TestUtils.buildTextAttribute("type", "hostA");
    for (Constraint<Attribute> constraint : constraintList) {
      assertFalse(constraint.isAccepted(attr));
    }
  }
}