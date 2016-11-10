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
package storm.mesos;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static storm.mesos.util.PrettyProtobuf.taskStatusToString;

public class NimbusScheduler implements Scheduler {
  private MesosNimbus mesosNimbus;
  private CountDownLatch _registeredLatch = new CountDownLatch(1);
  public static final Logger LOG = LoggerFactory.getLogger(MesosNimbus.class);

  public NimbusScheduler(MesosNimbus mesosNimbus) {
    this.mesosNimbus = mesosNimbus;
  }

  public void waitUntilRegistered() throws InterruptedException {
    _registeredLatch.await();
  }

  @Override
  public void registered(final SchedulerDriver driver, FrameworkID id, MasterInfo masterInfo) {
    mesosNimbus.doRegistration(driver, id);

    // Completed registration, let anything waiting for us to do so continue
    _registeredLatch.countDown();
  }

  @Override
  public void reregistered(SchedulerDriver sd, MasterInfo info) {
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
  }

  @Override
  public void error(SchedulerDriver driver, String msg) {
    LOG.error("Received fatal error \nmsg: {} \nHalting process...", msg);
    try {
      mesosNimbus.shutdown();
    } catch (Exception e) {
      // Swallow. Nothing we can do about it now.
    }
    Runtime.getRuntime().halt(2);
  }

  @Override
  public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    mesosNimbus.resourceOffers(driver, offers);
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID id) {
    LOG.info("Offer rescinded. offerId: {}", id.getValue());
    mesosNimbus.offerRescinded(id);
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    String msg = String.format("Received status update: %s", taskStatusToString(status));
    switch (status.getState()) {
      case TASK_STAGING:
      case TASK_STARTING:
        LOG.debug(msg);
        break;
      case TASK_RUNNING:
        LOG.info(msg);
        break;
      case TASK_FINISHED:
      case TASK_FAILED:
      case TASK_KILLED:
      case TASK_LOST:
      case TASK_ERROR:
        LOG.info(msg);
        break;
      default:
        LOG.warn("Received unrecognized status update: {}", taskStatusToString(status));
        break;
    }
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID id) {
    LOG.warn("Lost slave id: {}", id.getValue());
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executor, SlaveID slave, int status) {
    LOG.warn("Mesos Executor lost: executor: {} slave: {} status: {}", executor.getValue(), slave.getValue(), status);
  }
}
