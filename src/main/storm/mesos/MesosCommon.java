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
package storm.mesos;

import backtype.storm.scheduler.TopologyDetails;
import com.google.common.base.Optional;
import org.apache.log4j.Logger;

import java.util.*;

public class MesosCommon {
  public static final Logger LOG = Logger.getLogger(MesosCommon.class);

  public static final String WORKER_CPU_CONF = "topology.mesos.worker.cpu";
  public static final String WORKER_MEM_CONF = "topology.mesos.worker.mem.mb";
  public static final String EXECUTOR_CPU_CONF = "topology.mesos.executor.cpu";
  public static final String EXECUTOR_MEM_CONF = "topology.mesos.executor.mem.mb";
  public static final String SUICIDE_CONF = "mesos.supervisor.suicide.inactive.timeout.secs";
  public static final String AUTO_START_LOGVIEWER_CONF = "supervisor.autostart.logviewer";
  // Should we prefix the Worker Task ID with a configurable string (as well as the topology name)?
  public static final String WORKER_NAME_PREFIX = "topology.mesos.worker.prefix";
  public static final String WORKER_NAME_PREFIX_DELIMITER = "topology.mesos.worker.prefix.delimiter";
  public static final String MESOS_COMPONENT_NAME_DELIMITER = "topology.mesos.component.name.delimiter";

  public static final double DEFAULT_WORKER_CPU = 1;
  public static final double DEFAULT_WORKER_MEM_MB = 1000;
  public static final double DEFAULT_EXECUTOR_CPU = 0.1;
  public static final double DEFAULT_EXECUTOR_MEM_MB = 500;
  public static final int DEFAULT_SUICIDE_TIMEOUT_SECS = 120;

  public static final String SUPERVISOR_ID = "supervisorid";
  public static final String ASSIGNMENT_ID = "assignmentid";
  public static final String DEFAULT_WORKER_NAME_PREFIX_DELIMITER = "_";
  public static final String DEFAULT_MESOS_COMPONENT_NAME_DELIMITER = " | ";

  public static String hostFromAssignmentId(String assignmentId, String delimiter) {
    final int last = assignmentId.lastIndexOf(delimiter);
    String host = assignmentId.substring(last + delimiter.length());
    LOG.debug("assignmentId=" + assignmentId + " host=" + host);
    return host;
  }

  public static String getWorkerPrefix(Map conf, TopologyDetails info) {
    Map topologyConf = getFullTopologyConfig(conf, info);
    String prefix = Optional.fromNullable((String) topologyConf.get(WORKER_NAME_PREFIX)).or("");
    return prefix + info.getName() + getWorkerPrefixDelimiter(conf);
  }

  public static String getWorkerPrefixDelimiter(Map conf) {
    return Optional.fromNullable((String) conf.get(WORKER_NAME_PREFIX_DELIMITER))
        .or(DEFAULT_WORKER_NAME_PREFIX_DELIMITER);
  }

  public static String getMesosComponentNameDelimiter(Map conf, TopologyDetails info) {
    Map topologyConf = getFullTopologyConfig(conf, info);
    return Optional.fromNullable((String) topologyConf.get(MESOS_COMPONENT_NAME_DELIMITER))
        .or(DEFAULT_MESOS_COMPONENT_NAME_DELIMITER);
  }

  public static String taskId(String nodeid, int port) {
    return nodeid + "-" + port;
  }

  public static String supervisorId(String nodeid, String topologyId) {
    return nodeid + "-" + topologyId;
  }

  public static boolean startLogViewer(Map conf) {
    return Optional.fromNullable((Boolean) conf.get(AUTO_START_LOGVIEWER_CONF)).or(true);
  }

  public static int portFromTaskId(String taskId) {
    int last = taskId.lastIndexOf("-");
    String port = taskId.substring(last + 1);
    return Integer.parseInt(port);
  }

  public static int getSuicideTimeout(Map conf) {
    return Optional.fromNullable((Number) conf.get(SUICIDE_CONF))
        .or(DEFAULT_SUICIDE_TIMEOUT_SECS).intValue();
  }

  public static Map getFullTopologyConfig(Map conf, TopologyDetails info) {
    Map ret = new HashMap(conf);
    ret.putAll(info.getConf());
    return ret;
  }

  public static double topologyWorkerCpu(Map conf, TopologyDetails info) {
    Map topologyConfig = getFullTopologyConfig(conf, info);
    return Optional.fromNullable((Number) topologyConfig.get(WORKER_CPU_CONF))
        .or(DEFAULT_WORKER_CPU).doubleValue();
  }

  public static double topologyWorkerMem(Map conf, TopologyDetails info) {
    Map topologyConfig = getFullTopologyConfig(conf, info);
    return Optional.fromNullable((Number) topologyConfig.get(WORKER_MEM_CONF))
        .or(DEFAULT_WORKER_MEM_MB).doubleValue();
  }

  public static double executorCpu(Map conf) {
    return Optional.fromNullable((Number) conf.get(EXECUTOR_CPU_CONF))
        .or(DEFAULT_EXECUTOR_CPU).doubleValue();
  }

  public static double executorMem(Map conf) {
    return Optional.fromNullable((Number) conf.get(EXECUTOR_MEM_CONF))
        .or(DEFAULT_EXECUTOR_MEM_MB).doubleValue();
  }
}
