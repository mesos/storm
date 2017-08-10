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

import org.apache.storm.daemon.supervisor.Supervisor;
import org.apache.storm.scheduler.ISupervisor;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import clojure.lang.PersistentVector;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.logviewer.LogViewerController;
import storm.mesos.util.MesosCommon;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class MesosSupervisor implements ISupervisor {
  public static final Logger LOG = LoggerFactory.getLogger(MesosSupervisor.class);

  volatile String _executorId = null;
  volatile String _supervisorId = null;
  volatile String _assignmentId = null;
  volatile ExecutorDriver _driver;
  StormExecutor _executor;
  Map _conf;
  // Store state on port assignments arriving from MesosNimbus as task-launching requests.
  private static final TaskAssignments _taskAssignments = TaskAssignments.getInstance();

  public static void main(String[] args) {
    Map<String, Object> conf = ConfigUtils.readStormConfig();

    try {
      Supervisor supervisor = new Supervisor(conf, null, new MesosSupervisor());

      // We use reflection to call the private method, 'launchDaemon', which calls 'launch'
      Method m = Supervisor.class.getDeclaredMethod("launchDaemon");
      m.setAccessible(true);
      m.invoke(supervisor);
    } catch (Exception e) {
      String msg = String.format("main: Exception: %s", e.getMessage());
      LOG.error(msg);
      e.printStackTrace();
    }
  }

  /**
   * This method is no longer called by the Supervisor since it was refactored from Clojure to Java,
   * starting in Storm 1.0.3. Now, port changes get captured in calls to 'confirmAssigned'.
   */
  @Override
  public void assigned(Collection<Integer> ports) {
  }

  @Override
  public void prepare(Map conf, String localDir) {
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
   * Called by storm-core supervisor to determine if the port is assigned to this
   * supervisor, and thus whether a corresponding worker process should be
   * killed or started.
   */
  @Override
  public boolean confirmAssigned(int port) {
    return _taskAssignments.confirmAssigned(port);
  }

  @Override
  public Object getMetadata() {
    /*
     * Convert obtained Set into a List for 2 reasons:
     *  (1) Ensure returned object is serializable as required by storm's serialization
     *      of the SupervisorInfo while heartbeating.
     *      Previous to this change we were returning a ConcurrentHashMap$KeySetView,
     *      which is not necessarily serializable:
     *         http://bugs.java.com/bugdatabase/view_bug.do?bug_id=4756277
     *  (2) Ensure we properly instantiate the PersistentVector with all ports.
     *      If given a Set the PersistentVector.create() method will simply wrap the passed
     *      Set as a single element in the new vector. Whereas if you pass a java.util.List
     *      or clojure.lang.ISeq, then you get a true vector composed of the elements of the
     *      List or ISeq you passed.
     */
    List ports = new ArrayList(_taskAssignments.getAssignedPorts());
    if (ports == null) {
      return null;
    }
    return PersistentVector.create(ports);
  }

  @Override
  public String getSupervisorId() {
    return _supervisorId;
  }

  @Override
  public String getAssignmentId() {
    return MesosCommon.hostFromAssignmentId(_assignmentId, MesosCommon.getWorkerPrefixDelimiter(_conf));
  }

  @Override
  public void killedWorker(int port) {
    LOG.info("killedWorker: executor {} removing port {} assignment and sending " +
        "TASK_FINISHED update to Mesos", _executorId, port);
    TaskID taskId = _taskAssignments.deregister(port);
    if (taskId == null) {
      LOG.error("killedWorker: Executor {} failed to find TaskID for port {}, so not " +
          "issuing TaskStatus update to Mesos for this dead task.", _executorId, port);
      return;
    }
    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FINISHED)
        .setTaskId(taskId)
        .build();
    _driver.sendStatusUpdate(status);
  }

  protected boolean startLogViewer(Map conf) {
    return MesosCommon.autoStartLogViewer(conf);
  }

  class StormExecutor implements Executor {
    private CountDownLatch _registeredLatch = new CountDownLatch(1);

    public void waitUntilRegistered() throws InterruptedException {
      _registeredLatch.await();
    }

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
      LOG.info("Received executor data <{}>", executorInfo.getData().toStringUtf8());
      Map ids = (Map) JSONValue.parse(executorInfo.getData().toStringUtf8());
      _executorId = executorInfo.getExecutorId().getValue();
      _supervisorId = (String) ids.get(MesosCommon.SUPERVISOR_ID);
      _assignmentId = (String) ids.get(MesosCommon.ASSIGNMENT_ID);
      LOG.info("Registered supervisor with Mesos: {}, {} ", _supervisorId, _assignmentId);

      // Completed registration, let anything waiting for us to do so continue
      _registeredLatch.countDown();
    }


    @Override
    public void launchTask(ExecutorDriver driver, TaskInfo task) {
      try {
        int port = _taskAssignments.register(task.getTaskId());
        LOG.info("Executor {} received task assignment for port {}. Mesos TaskID: {}",
            _executorId, port, task.getTaskId().getValue());
      } catch (IllegalArgumentException e) {
        String msg =
            String.format("launchTask: failed to register task. " +
                          "Exception: %s Halting supervisor process.",
                          e.getMessage());
        LOG.error(msg);
        TaskStatus status = TaskStatus.newBuilder()
            .setState(TaskState.TASK_FAILED)
            .setTaskId(task.getTaskId())
            .setMessage(msg)
            .build();
        driver.sendStatusUpdate(status);
        Runtime.getRuntime().halt(1);
      }
      LOG.info("Received task assignment for TaskID: {} ",
          task.getTaskId().getValue());
      TaskStatus status = TaskStatus.newBuilder()
          .setState(TaskState.TASK_RUNNING)
          .setTaskId(task.getTaskId())
          .build();
      driver.sendStatusUpdate(status);
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID id) {
      LOG.warn("killTask not implemented in executor {}, so " +
          "cowardly refusing to kill task {}", _executorId, id.getValue());
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
      LOG.warn("shutdown not implemented in executor {}, so " +
          "cowardly refusing to kill tasks", _executorId);
    }

    @Override
    public void error(ExecutorDriver driver, String msg) {
      LOG.error("Received fatal error \nmsg: {} \nHalting process...", msg);
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
          if (!_taskAssignments.getAssignedPorts().isEmpty()) {
            _lastTime = now;
          }
          if ((now - _lastTime) > 1000L * _timeoutSecs) {
            LOG.info("Supervisor has not had anything assigned for {} secs. Committing suicide...", _timeoutSecs);
            Runtime.getRuntime().halt(0);
          }
          Utils.sleep(5000);
        }
      } catch (Throwable t) {
        LOG.error(t.getMessage());
        Runtime.getRuntime().halt(2);
      }
    }
  }
}
