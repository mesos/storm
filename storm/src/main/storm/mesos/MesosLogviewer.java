package storm.mesos;

import org.apache.mesos.*;
import org.apache.mesos.Protos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.Config;
import java.util.*;
import java.io.File;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * Created by rtang on 7/17/17.
 */

public class MesosLogviewer implements Executor {
  public static final Logger LOG = LoggerFactory.getLogger(MesosLogviewer.class);
  protected Process proc;
  TaskID taskId;
  ExecutorDriver driver;

  public static void main(String[] args) throws Exception {
    Executor executor = new MesosLogviewer();
    ExecutorDriver driver = new MesosExecutorDriver(executor);
    driver.run();
  }

  /* TODO: Should try to find some config which tells us where storm executable is instead of hardcode */
  private Process launchProcess() throws Exception {
    ProcessBuilder pb = new ProcessBuilder(
            Paths.get(System.getProperty("user.dir"), "/bin/storm").toString(),
            "logviewer");
    return pb.start();
  }

  private void statusUpdate(TaskState state) {
    TaskStatus status = TaskStatus.newBuilder()
            .setTaskId(taskId)
            .setState(state)
            .build();
    driver.sendStatusUpdate(status);
  }

  @Override
  public void launchTask(ExecutorDriver driver, TaskInfo task) {
    try {
      this.taskId = task.getTaskId();
      this.driver = driver;
      statusUpdate(TaskState.TASK_STARTING);

      this.proc = launchProcess();
      statusUpdate(TaskState.TASK_RUNNING);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {
    LOG.info("Received executor data <{}>", executorInfo.getData().toStringUtf8());
  }

  @Override
  public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
    LOG.info("Logviewer has re-registered with the mesos-slave");
  }

  @Override
  public void disconnected(ExecutorDriver driver) {
    LOG.info("Logviewer has disconnected from the mesos-slave");
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID id) {
    LOG.warn("killTask not implemented");
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] data) {
  }

  @Override
  public void shutdown(ExecutorDriver driver) {
    LOG.warn("shutdown not implemented");
  }

  @Override
  public void error(ExecutorDriver driver, String msg) {
    LOG.error("Received fatal error \nmsg: {} \nHalting process...", msg);
    Runtime.getRuntime().halt(2);
  }
}
