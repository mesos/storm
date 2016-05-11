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
package storm.mesos.resources;

import java.util.Comparator;
import java.util.List;

/**
 * An interface for managing various kinds of resources defined
 * in {@link storm.mesos.resources.ResourceType}
 */
public interface Resource<T extends ResourceEntry<? extends Number>> {

  public boolean isAvailable(T resourceEntry);

  public boolean isAvailable(T resourceEntry,  ReservationType reservationType);

  public List<? extends ResourceEntry> getAllAvailableResources();

  public List<? extends ResourceEntry> getAllAvailableResources(ReservationType reservationType);

  public void add(T resourceEntry, ReservationType reservationType);

  public List<ResourceEntry> removeAndGet(T resourceEntry) throws ResourceNotAvailabeException;

  public List<ResourceEntry> removeAndGet(T value, ReservationType reservationType) throws ResourceNotAvailabeException;

  public List<ResourceEntry> removeAndGet(T value, Comparator<ReservationType> reservationTypeComparator) throws ResourceNotAvailabeException;

}
