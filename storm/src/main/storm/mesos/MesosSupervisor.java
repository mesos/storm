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

import backtype.storm.generated.JavaObject;
import backtype.storm.generated.JavaObjectArg;
import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.json.simple.JSONValue;
import storm.mesos.logviewer.LogViewerController;
import storm.mesos.shims.ILocalStateShim;
import storm.mesos.shims.LocalStateShim;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import clojure.lang.PersistentVector;

public class MesosSupervisor implements ISupervisor {
  public static final Logger LOG = Logger.getLogger(MesosSupervisor.class);

  volatile String _id = null;
  volatile String _assignmentId = null;
  volatile ExecutorDriver _driver;
  StormExecutor _executor;
  ILocalStateShim _state;
  Map _conf;
  AtomicReference<Set<Integer>> _myassigned = new AtomicReference<Set<Integer>>(new HashSet<Integer>());

  public static void main(String[] args) {
    backtype.storm.daemon.supervisor.launch(new MesosSupervisor());
  }

  @Override
  public void assigned(Collection<Integer> ports) {
    if (ports == null) ports = new HashSet<>();
    _myassigned.set(new HashSet<>(ports));
  }

  @Override
  public void prepare(Map conf, String localDir) {
    try {
      _state = new LocalStateShim(localDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    _executor = new StormExecutor();
    _driver = new MesosExecutorDriver(_executor);
    _driver.start();
    LOG.info("Waiting for executor to initialize...");
    _conf = conf;
    try {
      _executor.waitUntilRegistered();

      if (startLogViewer(conf)) {
        LOG.info("Starting logviewer...");
        LogViewerController logController = new LogViewerController(conf);
        logController.start();
      }

    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Executor initialized...");
    Thread suicide = new SuicideDetector(conf);
    suicide.setDaemon(true);
    suicide.start();
  }

  /**
   * Called by supervisor core to determine if the port is assigned to this
   * supervisor, and thus whether a corresponding worker process should
   * be killed or started.
   */
  @Override
  public boolean confirmAssigned(int port) {
    String val = _state.get(Integer.toString(port));
    return val != null;
  }

  @Override
  public Object getMetadata() {
    Object[] ports = _state.snapshot().keySet().toArray();
    Integer[] p = new Integer[ports.length];
    for (int i = 0; i < ports.length; i++) {
      p[i] = Integer.parseInt((String) ports[i]);
    }
    return PersistentVector.create((Object[]) p);
  }

  @Override
  public String getSupervisorId() {
    return _id;
  }

  @Override
  public String getAssignmentId() {
    return MesosCommon.hostFromAssignmentId(_assignmentId, MesosCommon.getWorkerPrefixDelimiter(_conf));
  }

  @Override
  public void killedWorker(int port) {
    LOG.info("killedWorker: removing port " + port + " from the 'assigned port state'");
    String taskId = _state.get(Integer.toString(port));
    _state.remove(Integer.toString(port));
    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FINISHED)
        .setTaskId(TaskID.newBuilder().setValue(taskId))
        .build();
    _driver.sendStatusUpdate(status);
  }

  protected boolean startLogViewer(Map conf) {
    return MesosCommon.startLogViewer(conf);
  }

  class StormExecutor implements Executor {
    private CountDownLatch _registeredLatch = new CountDownLatch(1);

    public void waitUntilRegistered() throws InterruptedException {
      _registeredLatch.await();
    }

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
      LOG.info("Received executor data <" + executorInfo.getData().toStringUtf8() + ">");
      Map ids = (Map) JSONValue.parse(executorInfo.getData().toStringUtf8());
      _id = (String) ids.get(MesosCommon.SUPERVISOR_ID);
      _assignmentId = (String) ids.get(MesosCommon.ASSIGNMENT_ID);
      LOG.info("Registered supervisor with Mesos: " + _id + ", " + _assignmentId);

      // Completed registration, let anything waiting for us to do so continue
      _registeredLatch.countDown();
    }


    @Override
    public void launchTask(ExecutorDriver driver, TaskInfo task) {
      int port = 0;
      try {
        port = MesosCommon.portFromTaskId(task.getTaskId().getValue());
      } catch (IllegalArgumentException e) {
        String msg = "launchTask: failed to extract port from TaskID: " +
            task.getTaskId().getValue() + ". Halting supervisor process.";
        LOG.error(msg);
        TaskStatus status = TaskStatus.newBuilder()
            .setState(TaskState.TASK_FAILED)
            .setTaskId(task.getTaskId())
            .setMessage(msg)
            .build();
        driver.sendStatusUpdate(status);
        Runtime.getRuntime().halt(1);
      }

      LOG.info("Received task assignment for port " + port + ". Mesos TaskID: " +
          task.getTaskId().getValue());
      // Record TaskID to be used later for sending a TASK_FINISHED update
      // when the worker process is killed.
      _state.put(Integer.toString(port), task.getTaskId().getValue());
      TaskStatus status = TaskStatus.newBuilder()
          .setState(TaskState.TASK_RUNNING)
          .setTaskId(task.getTaskId())
          .build();
      driver.sendStatusUpdate(status);
    }

    /**
     * If a failure occurs we halt this process, to avoid having an inconsistency
     * between Mesos's view of the running tasks and which processes are actually
     * running.
     * Killing this supervisor process also kills any child worker processes.
     */
    @Override
    public void killTask(ExecutorDriver driver, TaskID id) {
      int port = 0;
      try {
        port = MesosCommon.portFromTaskId(id.getValue());
      } catch (IllegalArgumentException e) {
        LOG.error("killTask: Halting executor process because we had a problem" +
            " extracting the port from the TaskID: " + id.getValue(), e);
        Runtime.getRuntime().halt(1);
      }

      LOG.info("killTask: killing task " + id.getValue() +
          " which is running on port " + port);
      _state.remove(Integer.toString(port));
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
      LOG.info("executor is being shutdown");
    }

    @Override
    public void error(ExecutorDriver driver, String msg) {
      LOG.error("Received fatal error \nmsg:" + msg + "\nHalting process...");
      Runtime.getRuntime().halt(2);
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
      LOG.info("executor has reregistered with the mesos-slave");
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
      LOG.info("executor has disconnected from the mesos-slave");
    }

  }

  public class SuicideDetector extends Thread {
    long _lastTime = System.currentTimeMillis();
    int _timeoutSecs;

    public SuicideDetector(Map conf) {
      _timeoutSecs = MesosCommon.getSuicideTimeout(conf);
    }

    @Override
    public void run() {
      try {
        while (true) {
          long now = System.currentTimeMillis();
          if (!_myassigned.get().isEmpty()) {
            _lastTime = now;
          }
          if ((now - _lastTime) > 1000L * _timeoutSecs) {
            LOG.info("Supervisor has not had anything assigned for " + _timeoutSecs + " secs. Committing suicide...");
            Runtime.getRuntime().halt(0);
          }
          Utils.sleep(5000);
        }
      } catch (Throwable t) {
        LOG.error(t);
        Runtime.getRuntime().halt(2);
      }
    }
  }
}
