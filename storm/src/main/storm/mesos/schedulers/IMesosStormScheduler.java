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
package storm.mesos.schedulers;


import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.WorkerSlot;
import org.apache.mesos.Protos;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A scheduler needs to implement the following interface for it to be MesosNimbus compatible.
 */
public interface IMesosStormScheduler {

  /**
   * This method is invoked by Nimbus when it wants to get a list of worker slots that are available for assigning the
   * topology workers. In Nimbus's view, a "WorkerSlot" is a host and port that it can use to assign a worker.
   */
  public List<WorkerSlot> allSlotsAvailableForScheduling(Map<Protos.OfferID, Protos.Offer> offers,
                                                         Collection<SupervisorDetails> existingSupervisors,
                                                         Topologies topologies, Set<String> topologiesMissingAssignments);
}
