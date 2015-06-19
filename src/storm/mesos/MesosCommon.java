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

import backtype.storm.scheduler.TopologyDetails;
import org.apache.log4j.Logger;

import java.util.*;

public class MesosCommon {
  public static final Logger LOG = Logger.getLogger(MesosCommon.class);

  public static final String WORKER_CPU_CONF = "topology.mesos.worker.cpu";
  public static final String WORKER_MEM_CONF = "topology.mesos.worker.mem.mb";
  public static final String EXECUTOR_CPU_CONF = "topology.mesos.executor.cpu";
  public static final String EXECUTOR_MEM_CONF = "topology.mesos.executor.mem.mb";
  public static final String SUICIDE_CONF = "mesos.supervisor.suicide.inactive.timeout.secs";
  public static final String DOCKER_IMAGE_CONF = "mesos.executor.docker.image";
  public static final String AUTO_START_LOGVIEWER_CONF = "supervisor.autostart.logviewer";

  public static final double DEFAULT_CPU = 1;
  public static final double DEFAULT_MEM_MB = 1000;
  public static final int DEFAULT_SUICIDE_TIMEOUT_SECS = 120;

  public static final String SUPERVISOR_ID = "supervisorid";
  public static final String ASSIGNMENT_ID = "assignmentid";

  public static String taskId(String nodeid, int port) {
    return nodeid + "-" + port;
  }

  public static String supervisorId(String nodeid, String topologyId) {
    return nodeid + "-" + topologyId;
  }

  public static int portFromTaskId(String taskId) {
    int last = taskId.lastIndexOf("-");
    String port = taskId.substring(last + 1);
    return Integer.parseInt(port);
  }

  public static int getSuicideTimeout(Map conf) {
    Number timeout = (Number) conf.get(SUICIDE_CONF);
    if (timeout == null) {
        return DEFAULT_SUICIDE_TIMEOUT_SECS;
    } else {
        return timeout.intValue();
    }
  }

  public static Map getFullTopologyConfig(Map conf, TopologyDetails info) {
    Map ret = new HashMap(conf);
    ret.putAll(info.getConf());
    return ret;
  }

  public static double topologyWorkerCpu(Map conf, TopologyDetails info) {
    conf = getFullTopologyConfig(conf, info);
    Object cpuObj = conf.get(WORKER_CPU_CONF);
    if (cpuObj != null && !(cpuObj instanceof Number)) {
      LOG.warn("Topology has invalid mesos cpu configuration: " + cpuObj + " for topology " + info.getId());
      cpuObj = null;
    }
    if (cpuObj == null) {
        return DEFAULT_CPU;
    } else {
        return ((Number) cpuObj).doubleValue();
    }
  }

  public static double topologyWorkerMem(Map conf, TopologyDetails info) {
    conf = getFullTopologyConfig(conf, info);
    Object memObj = conf.get(WORKER_MEM_CONF);
    if (memObj != null && !(memObj instanceof Number)) {
      LOG.warn("Topology has invalid mesos mem configuration: " + memObj + " for topology " + info.getId());
      memObj = null;
    }
    if (memObj == null) { 
        return DEFAULT_MEM_MB;
    } else {
        return ((Number) memObj).doubleValue();
    }
  }

  public static double executorCpu(Map conf) {
    Object cpuObj = conf.get(EXECUTOR_CPU_CONF);
    if (cpuObj != null && !(cpuObj instanceof Number)) {
      LOG.warn("Cluster has invalid mesos cpu configuration: " + cpuObj);
      cpuObj = null;
    }
    if (cpuObj == null) {
        return DEFAULT_CPU;
    } else {
        return ((Number) cpuObj).doubleValue();
    }
  }

  public static double executorMem(Map conf) {
    Object memObj = conf.get(EXECUTOR_MEM_CONF);
    if (memObj != null && !(memObj instanceof Number)) {
      LOG.warn("Cluster has invalid mesos mem configuration: " + memObj);
      memObj = null;
    }
    if (memObj == null) {
        return DEFAULT_MEM_MB;
    } else {
        return ((Number) memObj).doubleValue();
    }
  }
  public static int numWorkers(Map conf, TopologyDetails info) {
    return info.getNumWorkers();
  }

  public static List<String> getTopologyIds(Collection<TopologyDetails> details) {
    List<String> ret = new ArrayList();
    for (TopologyDetails d : details) {
      ret.add(d.getId());
    }
    return ret;
  }
}
