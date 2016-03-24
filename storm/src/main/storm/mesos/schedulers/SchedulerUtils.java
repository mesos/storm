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

import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.TopologyDetails;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SchedulerUtils {

  private static final Logger log = LoggerFactory.getLogger(SchedulerUtils.class);

  public static boolean isFit(Map mesosStormConf, OfferResources offerResources, TopologyDetails topologyDetails, boolean supervisorExists) {
    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    requestedWorkerCpu += supervisorExists ? 0 : MesosCommon.executorCpu(mesosStormConf);
    requestedWorkerMem += supervisorExists ? 0 : MesosCommon.executorMem(mesosStormConf);

    if (requestedWorkerCpu <= offerResources.getCpu() && requestedWorkerMem <= offerResources.getMem()) {
      return true;
    }
    return false;
  }

  public static boolean isFit(Map mesosStormConf, Protos.Offer offer, TopologyDetails topologyDetails, boolean supervisorExists) {
    OfferResources offerResources = new OfferResources(offer);
    return isFit(mesosStormConf, offerResources, topologyDetails, supervisorExists);
  }

  /**
   * Method checks if all topologies that need assignment already have supervisor running on the node where the Offer
   * comes from. Required for more accurate available resource calculation where we can exclude supervisor's demand from
   * the Offer.
   * Unfortunately because of WorkerSlot type is not topology agnostic, we need to exclude supervisor's resources only
   * in case where ALL topologies in 'allSlotsAvailableForScheduling' method satisfy condition of supervisor existence
   * @param offerHost hostname corresponding to the offer
   * @param existingSupervisors Supervisors which already placed on the node for the Offer
   * @param topologyId Topology id for which we are checking if the supervisor exists already
   * @return boolean value indicating supervisor existence
   */
  public static boolean supervisorExists(String offerHost, Collection<SupervisorDetails> existingSupervisors,
                                   String topologyId) {
    boolean supervisorExists = false;
    String expectedSupervisorId = MesosCommon.supervisorId(offerHost, topologyId);
    for (SupervisorDetails supervisorDetail : existingSupervisors) {
      if (supervisorDetail.getId().equals(expectedSupervisorId)) {
        supervisorExists = true;
      }
    }
    return supervisorExists;
  }

  public static Map<String, List<OfferResources>> getOfferResourcesListPerNode(RotatingMap<Protos.OfferID, Protos.Offer> offers) {
    Map<String, List<OfferResources>> offerResourcesListPerNode = new HashMap<>();

    for (Protos.Offer offer : offers.values()) {
      String hostName = offer.getHostname();

      List<OfferResources> offerResourcesListForCurrentHost = offerResourcesListPerNode.get(hostName);
      OfferResources offerResources = new OfferResources(offer);
      if (offerResourcesListForCurrentHost == null) {
        offerResourcesListPerNode.put(hostName, new ArrayList<OfferResources>());
      }
      offerResourcesListPerNode.get(hostName).add(offerResources);
      log.info("Available resources at {}: {}", hostName, offerResources.toString());
    }
    return offerResourcesListPerNode;
  }

  public static MesosWorkerSlot createWorkerSlotFromOfferResources(Map mesosStormConf, OfferResources offerResources,
                                                            TopologyDetails topologyDetails, boolean supervisorExists) {
    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    requestedWorkerCpu += supervisorExists ? 0 : MesosCommon.executorCpu(mesosStormConf);
    requestedWorkerMem += supervisorExists ? 0 : MesosCommon.executorMem(mesosStormConf);

    if (requestedWorkerCpu > offerResources.getCpu()) {
      log.warn("Refusing to create worker slot. requestedWorkerCpu: {} but OfferedCpu: {} at node: {}",
               requestedWorkerCpu, offerResources.getCpu(), offerResources.getHostName());
      return null;
    }

    if (requestedWorkerMem > offerResources.getMem()) {
      log.warn("Refusing to create worker slot. requestedWorkerMem: {} but OfferedMem: {} at node: {}",
               requestedWorkerMem, offerResources.getMem(), offerResources.getHostName());
      return null;
    }

    long port = offerResources.getPort();

    if (port == -1) {
      log.warn("Refusing to create worker slot. There are no ports available with offer {}", offerResources.toString());
      return null;
    }

    offerResources.decCpu(requestedWorkerCpu);
    offerResources.decMem(requestedWorkerMem);

    return new MesosWorkerSlot(offerResources.getHostName(), port, topologyDetails.getId());
  }
}
