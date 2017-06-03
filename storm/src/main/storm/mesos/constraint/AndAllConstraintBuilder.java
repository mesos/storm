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

import com.google.common.base.Optional;

import java.util.List;
import java.util.Map;

/**
 * @author fuji-151a
 */
public class AndAllConstraintBuilder<T> implements ConstraintBuilder<T> {

  private final MultiConstraintBuilder<T> builder;

  public AndAllConstraintBuilder(final MultiConstraintBuilder<T> multiConstraintBuilder) {
    this.builder = multiConstraintBuilder;
  }

  @Override
  public Optional<Constraint<T>> build(final Map conf) {
    List<Constraint<T>> ruleList = builder.build(conf);
    if (ruleList.isEmpty()) {
      return Optional.absent();
    }
    Constraint<T> one = ruleList.remove(0);
    if (ruleList.isEmpty()) {
      return Optional.of(one);
    }
    Constraint<T> two = ruleList.remove(0);
    return Optional.of(compose(one, two, ruleList));
  }

  private Constraint<T> compose(final Constraint<T> one,
                                final Constraint<T> two,
                                final List<Constraint<T>> tail) {
    Constraint<T> results = new AndConstraint<>(one, two);
    if (tail.isEmpty()) {
      return results;
    }
    Constraint<T> three = tail.remove(0);
    return compose(results, three, tail);
  }
}
