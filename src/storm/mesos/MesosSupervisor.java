package storm.mesos;

import backtype.storm.scheduler.ISupervisor;
import backtype.storm.utils.LocalState;
import backtype.storm.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

public class MesosSupervisor implements ISupervisor {
  public static final Logger LOG = Logger.getLogger(MesosSupervisor.class);

  volatile String _id = null;
  volatile String _assignmentId = null;
  volatile ExecutorDriver _driver;
  StormExecutor _executor;
  LocalState _state;

  AtomicReference<Set<Integer>> _myassigned = new AtomicReference<Set<Integer>>(new HashSet<Integer>());

  public static void main(String[] args) {
    backtype.storm.daemon.supervisor.launch(new MesosSupervisor());
  }

  @Override
  public void assigned(Collection<Integer> ports) {
    if (ports == null) ports = new HashSet<Integer>();
    _myassigned.set(new HashSet<Integer>(ports));
  }

  @Override
  public void prepare(Map conf, String localDir) {
    try {
      _state = new LocalState(localDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Semaphore initter = new Semaphore(0);
    _executor = new StormExecutor(initter);
    _driver = new MesosExecutorDriver(_executor);
    _driver.start();
    LOG.info("Waiting for executor to initialize...");
    try {
      initter.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    LOG.info("Executor initialized...");
    Thread suicide = new SuicideDetector(conf);
    suicide.setDaemon(true);
    suicide.start();
  }

  @Override
  public boolean confirmAssigned(int port) {
    try {
      Object val = _state.get(port);
      return val != null;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Object getMetadata() {
    return null;
  }

  @Override
  public String getSupervisorId() {
    return _id;
  }

  @Override
  public String getAssignmentId() {
    return _assignmentId;
  }

  @Override
  public void killedWorker(int port) {
    try {
      _state.remove(port);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    TaskStatus status = TaskStatus.newBuilder()
        .setState(TaskState.TASK_FINISHED)
        .setTaskId(TaskID.newBuilder().setValue(MesosCommon.taskId(_assignmentId, port)))
        .build();
    _driver.sendStatusUpdate(status);
  }

  class StormExecutor implements Executor {

    Semaphore initter;

    public StormExecutor(Semaphore onInitialization) {
      initter = onInitialization;
    }

    @Override
    public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
      LOG.info("Received executor data <" + executorInfo.getData().toStringUtf8() + ">");
      Map ids = (Map) JSONValue.parse(executorInfo.getData().toStringUtf8());
      _id = (String) ids.get(MesosCommon.SUPERVISOR_ID);
      _assignmentId = (String) ids.get(MesosCommon.ASSIGNMENT_ID);
      LOG.info("Registered supervisor with Mesos: " + _id + ", " + _assignmentId);
      initter.release();
    }


    @Override
    public void launchTask(ExecutorDriver driver, TaskInfo task) {
      int port = MesosCommon.portFromTaskId(task.getTaskId().getValue());
      try {
        LOG.info("Received task assignment for port " + port);
        _state.put(port, true);
      } catch (IOException e) {
        LOG.error("Halting process...", e);
        Runtime.getRuntime().halt(1);
      }
      TaskStatus status = TaskStatus.newBuilder()
          .setState(TaskState.TASK_RUNNING)
          .setTaskId(task.getTaskId())
          .build();
      driver.sendStatusUpdate(status);
    }

    @Override
    public void killTask(ExecutorDriver driver, TaskID id) {
    }

    @Override
    public void frameworkMessage(ExecutorDriver driver, byte[] data) {
    }

    @Override
    public void shutdown(ExecutorDriver driver) {
    }

    @Override
    public void error(ExecutorDriver driver, String msg) {
      LOG.error("Received fatal error \nmsg:" + msg + "\nHalting process...");
      Runtime.getRuntime().halt(2);
    }

    @Override
    public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    }

    @Override
    public void disconnected(ExecutorDriver driver) {
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
