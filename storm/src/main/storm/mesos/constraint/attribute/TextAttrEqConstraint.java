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
