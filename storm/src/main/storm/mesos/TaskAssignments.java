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

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mesos.Protos.TaskID;

import storm.mesos.util.MesosCommon;

/**
 * Tracks the Mesos Tasks / Storm Worker Processes that have been assigned
 * to this MesosSupervisor instance.
 *
 * Serves as a bridge between the storm-core supervisor logic and the
 * mesos executor logic.
 *
 * Tasks are assigned or removed via either:
 *   Storm (storm-core supervisor) calling MesosSupervisor's ISupervisor interfaces
 *   Mesos (mesos executor driver) calling MesosSupervisor's Executor interfaces
 *
 * This abstraction allows the MesosSupervisor code to stay a bit tighter and cleaner,
 * while still giving the power to track the TaskID across deactivation of a port.
 * That allows calls through the Mesos interfaces to deactivate a port and then have
 * the storm-core supervisor see that the port is no longer active (via false return from
 * ISupervisor.confirmAssigned), and thus drives the storm-core supervisor to kill the
 * worker process associated with the port.
 */
public enum TaskAssignments {
  INSTANCE; // This is a singleton class; see Effective Java (2nd Edition): Item 3

  // Alternative to a constructor for this enum-based singleton
  public static TaskAssignments getInstance() {
    return INSTANCE;
  }

  private enum TaskState {
    ACTIVE,
    INACTIVE
  }

  // NOTE: this doesn't *really* need to be Serializable -- this is only done to avoid
  // an exception thrown during mk-supervisor if the ISupervisor object isn't serializable.
  private static class AssignmentInfo implements Serializable {
    public final transient TaskID taskId;
    public final transient TaskState taskState;

    public AssignmentInfo(TaskID taskId, TaskState taskState) {
      this.taskId = taskId;
      this.taskState = taskState;
    }
  }

  // Map of ports to their assigned tasks' info.
  private final Map<Integer, AssignmentInfo> portAssignments = new ConcurrentHashMap<>();

  public Set<Integer> getAssignedPorts() {
    return portAssignments.keySet();
  }

  /**
   * Signal to the storm-core supervisor that this task should be started.
   */
  public int register(TaskID taskId) throws IllegalArgumentException {
    int port = MesosCommon.portFromTaskId(taskId.getValue());
    AssignmentInfo existingAssignment = portAssignments.get(port);
    if (existingAssignment != null) {
      throw new IllegalArgumentException("Refusing to register task " + taskId.getValue() +
                        " because its port " + port + " is already registered for task " +
                        existingAssignment.taskId.getValue());
    }
    portAssignments.put(port, new AssignmentInfo(taskId, TaskState.ACTIVE));
    return port;
  }

  /**
   * This task has been killed by the storm-core supervisor, ditch all
   * knowledge of it.
   */
  public TaskID deregister(int port) {
    AssignmentInfo assignment = portAssignments.remove(port);
    if (assignment != null) {
      return assignment.taskId;
    }
    return null;
  }

  /**
   * Is this port active? (And thus also registered?)
   *
   * Used by storm-core supervisor to determine if a worker process should be
   * launched or killed on the specified port.
   */
  public boolean confirmAssigned(int port) {
    AssignmentInfo assignment = portAssignments.get(port);
    if (assignment != null) {
      return (assignment.taskState == TaskState.ACTIVE);
    }
    return false;
  }

  /**
   * Signal to the storm-core supervisor that this task should be killed.
   *
   * This method provides a way for Mesos (well, calls routed through Mesos to the Executor
   * interface implemented by MesosSupervisor) to kill the worker processes (mesos tasks).
   *
   * This works by changing the state of an existing assignment from active to inactive,
   * thus signaling to the storm-core supervisor that the assignment is no longer active.
   * And that works by the storm-core supervisor checking the active/inactive state via a
   * periodic call to ISupervisor.confirmAssigned(), which calls TaskAssignments.confirmAssigned().
   * Notably, we must retain *some* knowledge about the assignment despite it being deactivated,
   * namely the TaskID, so that when the storm-core supervisor calls ISupervisor.killedWorker()
   * we can send a TASK_FINISHED TaskStatus update to Mesos. That is required for Mesos to
   * learn of the task having been killed, otherwise Mesos would think the task is still running.
   *
   * NOTE: This isn't used *yet*, as we haven't spent time to code and test
   * MesosSupervisor.killTask() and MesosSupervisor.shutdown().
   */
  public int deactivate(TaskID taskId) throws IllegalArgumentException {
    int port = MesosCommon.portFromTaskId(taskId.getValue());
    AssignmentInfo assignment = portAssignments.get(port);
    if (assignment != null) {
      portAssignments.put(port, new AssignmentInfo(taskId, TaskState.INACTIVE));
      return port;
    }
    return 0;
  }

}
