/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos.schedulers;

import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ResourceNotAvailableException;
import storm.mesos.resources.ResourceType;
import storm.mesos.util.MesosCommon;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static storm.mesos.resources.ResourceEntries.RangeResourceEntry;
import static storm.mesos.resources.ResourceEntries.ScalarResourceEntry;

public class SchedulerUtils {

  private static final Logger log = LoggerFactory.getLogger(SchedulerUtils.class);

  public static List<RangeResourceEntry> getPorts(AggregatedOffers aggregatedOffers, int requiredCount) {
    List<RangeResourceEntry> retVal = new ArrayList<>();
    List<RangeResourceEntry> resourceEntryList = aggregatedOffers.getAllAvailableResources(ResourceType.PORTS);

    for (RangeResourceEntry rangeResourceEntry : resourceEntryList) {
      Long begin = rangeResourceEntry.getBegin();
      Long end = rangeResourceEntry.getEnd();
      while (begin <= end && requiredCount > 0) {
        retVal.add(new RangeResourceEntry(begin, begin));
        ++begin;
        --requiredCount;
      }
      if (requiredCount <= 0) {
        break;
      }
    }
    return retVal;
  }

  public static MesosWorkerSlot createMesosWorkerSlot(Map mesosStormConf,
                                               AggregatedOffers aggregatedOffers,
                                               TopologyDetails topologyDetails,
                                               boolean supervisorExists) throws ResourceNotAvailableException {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    requestedWorkerCpu += supervisorExists ? 0 : MesosCommon.executorCpu(mesosStormConf);
    requestedWorkerMem += supervisorExists ? 0 : MesosCommon.executorMem(mesosStormConf);

    aggregatedOffers.reserve(ResourceType.CPU, new ScalarResourceEntry(requestedWorkerCpu));
    aggregatedOffers.reserve(ResourceType.MEM, new ScalarResourceEntry(requestedWorkerMem));

    List<RangeResourceEntry> ports = getPorts(aggregatedOffers, 1);
    if (ports.isEmpty()) {
      throw new ResourceNotAvailableException("No ports available to create MesosWorkerSlot.");
    }
    aggregatedOffers.reserve(ResourceType.PORTS, ports.get(0));

    return new MesosWorkerSlot(aggregatedOffers.getHostname(), ports.get(0).getBegin(), topologyDetails.getId());
  }

  /**
   * Check if this topology already has a supervisor running on the node where the Offer
   * comes from. Required to account for supervisor/mesos-executor's resource needs.
   * Note that there is one-and-only-one supervisor per topology per node.
   *
   * @param offerHost host that sent this Offer
   * @param existingSupervisors List of supervisors which already exist on the Offer's node
   * @param topologyId ID of topology requiring assignment
   * @return boolean value indicating supervisor existence
   */
  public static boolean supervisorExists(String frameworkName, String offerHost, Collection<SupervisorDetails> existingSupervisors,
                                   String topologyId) {
    String expectedSupervisorId = MesosCommon.supervisorId(frameworkName, offerHost, topologyId);
    for (SupervisorDetails supervisorDetail : existingSupervisors) {
      if (supervisorDetail.getId().equals(expectedSupervisorId)) {
        return true;
      }
    }
    return false;
  }
}
