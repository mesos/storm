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

import backtype.storm.scheduler.IScheduler;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.schedulers.StormSchedulerImpl;
import storm.mesos.util.ZKClient;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static storm.mesos.util.PrettyProtobuf.taskStatusToString;

public class NimbusMesosScheduler implements Scheduler {
  private MesosNimbus mesosNimbus;
  private ZKClient zkClient;
  private String logviewerZkDir;
  private CountDownLatch _registeredLatch = new CountDownLatch(1);
  public static final Logger LOG = LoggerFactory.getLogger(MesosNimbus.class);
  public static final String DEFAULT_MESOS_COMPONENT_NAME_DELIMITER = "|";

  public NimbusMesosScheduler(MesosNimbus mesosNimbus, ZKClient zkClient, String logviewerZkDir) {
    this.mesosNimbus = mesosNimbus;
    this.zkClient = zkClient;
    this.logviewerZkDir = logviewerZkDir;
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
    if (status.getTaskId().getValue().contains("logviewer")) {
      updateLogviewerState(status);
    }
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

  private void updateLogviewerState(TaskStatus status) {
    String nodeId = status.getTaskId().getValue().split("\\" + DEFAULT_MESOS_COMPONENT_NAME_DELIMITER)[0];
    switch (status.getState()) {
      case TASK_STAGING: return;
      case TASK_STARTING: return;
      case TASK_RUNNING: return;
      default:
    }
    // if it gets to this point it means logviewer terminated; update ZK with new logviewer state
    String logviewerZKPath = String.format("%s/%s", logviewerZkDir, nodeId);
    if (zkClient.nodeExists(logviewerZKPath)) {
      LOG.info("updateLogviewerState: Remove logviewer state in zk: {}", logviewerZKPath);
      zkClient.deleteNode(logviewerZKPath);
      LOG.info("updateLogviewerState: Reviving offers for logviewer relaunch");
      mesosNimbus._driver.reviveOffers();
      StormSchedulerImpl stormScheduler = (StormSchedulerImpl) mesosNimbus.getForcedScheduler();
      stormScheduler.unsetOffersSuppressed();
    } else {
      LOG.error("Task exists for logviewer that isn't tracked in ZooKeeper");
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
