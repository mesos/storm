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
package storm.mesos.util;

import com.google.common.base.Optional;
import org.apache.mesos.Protos;
import org.apache.storm.Config;
import org.apache.storm.scheduler.TopologyDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.mesos.resources.AggregatedOffers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MesosCommon {
  public static final Logger LOG = LoggerFactory.getLogger(MesosCommon.class);

  public static final String WORKER_CPU_CONF = "topology.mesos.worker.cpu";
  public static final String WORKER_MEM_CONF = "topology.mesos.worker.mem.mb";
  public static final String EXECUTOR_CPU_CONF = "topology.mesos.executor.cpu";
  public static final String EXECUTOR_MEM_CONF = "topology.mesos.executor.mem.mb";
  public static final String SUICIDE_CONF = "mesos.supervisor.suicide.inactive.timeout.secs";
  public static final String SUPERVISOR_STORM_LOCAL_DIR_CONF = "mesos.supervisor.storm.local.dir";
  public static final String CONF_MESOS_ROLE = "mesos.framework.role";
  public static final String AUTO_START_LOGVIEWER_CONF = "supervisor.autostart.logviewer";
  // Should we prefix the Worker Task ID with a configurable string (as well as the topology name)?
  public static final String WORKER_NAME_PREFIX = "topology.mesos.worker.prefix";
  public static final String WORKER_NAME_PREFIX_DELIMITER = "topology.mesos.worker.prefix.delimiter";
  public static final String MESOS_COMPONENT_NAME_DELIMITER = "topology.mesos.component.name.delimiter";

  public static final double MESOS_MIN_CPU = 0.01;
  public static final double MESOS_MIN_MEM_MB = 32;
  public static final double DEFAULT_WORKER_CPU = 1;
  public static final double DEFAULT_WORKER_MEM_MB = 1000;
  public static final double DEFAULT_EXECUTOR_CPU = 0.1;
  public static final double DEFAULT_EXECUTOR_MEM_MB = 500;
  public static final int DEFAULT_SUICIDE_TIMEOUT_SECS = 120;
  public static final String DEFAULT_SUPERVISOR_STORM_LOCAL_DIR = "storm-local";

  public static final String SUPERVISOR_ID = "supervisorid";
  public static final String ASSIGNMENT_ID = "assignmentid";
  public static final String DEFAULT_WORKER_NAME_PREFIX_DELIMITER = "_";
  public static final String DEFAULT_MESOS_COMPONENT_NAME_DELIMITER = "|";

  public static String getNimbusHost(Map mesosStormConf) throws UnknownHostException {
    Optional<String> nimbusHostFromConfig =  Optional.fromNullable((String) mesosStormConf.get(Config.NIMBUS_HOST));
    Optional<String> nimbusHostFromEnv = Optional.fromNullable(System.getenv("MESOS_NIMBUS_HOST"));

    return nimbusHostFromConfig.or(nimbusHostFromEnv).or(InetAddress.getLocalHost().getCanonicalHostName());
  }

  public static String hostFromAssignmentId(String assignmentId, String delimiter) {
    final int last = assignmentId.lastIndexOf(delimiter);
    String host = assignmentId.substring(last + delimiter.length());
    LOG.debug("assignmentId={} host={}", assignmentId, host);
    return host;
  }

  public static String getWorkerPrefix(Map conf, TopologyDetails info) {
    Map topologyConf = getFullTopologyConfig(conf, info);
    String prefix = Optional.fromNullable((String) topologyConf.get(WORKER_NAME_PREFIX)).or("");
    return String.format("%s%s%s", prefix, info.getName(), getWorkerPrefixDelimiter(conf));
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

  public static String timestampMillis() {
    long now = System.currentTimeMillis();
    long secs = TimeUnit.MILLISECONDS.toSeconds(now);
    long msecs = now - TimeUnit.SECONDS.toMillis(secs);
    return String.format("%d.%03d", secs, msecs);
  }

  public static String taskId(String nodeid, int port) {
    return String.format("%s-%d-%s", nodeid, port, timestampMillis());
  }

  public static String supervisorId(String nodeid, String topologyId) {
    return String.format("%s%s%s", nodeid, DEFAULT_MESOS_COMPONENT_NAME_DELIMITER, topologyId);
  }

  public static boolean autoStartLogViewer(Map conf) {
    return Optional.fromNullable((Boolean) conf.get(AUTO_START_LOGVIEWER_CONF)).or(true);
  }

  public static int portFromTaskId(String taskId) {
    String[] parts = taskId.trim().split("-");
    if (parts.length < 3) {
      throw new IllegalArgumentException(String.format("TaskID %s is invalid. " +
          "Number of dash-delimited components (%d) is less than expected. " +
          "Expected format is HOSTNAME-PORT-TIMESTAMP", taskId.trim(), parts.length));
    }

    // TaskID format: HOSTNAME-PORT-TIMESTAMP. Notably, HOSTNAME can have dashes too,
    // so the port is the 2nd-to-last part after splitting on dash.
    String portString = parts[parts.length - 2];
    int port;
    try {
      port = Integer.parseInt(portString);
    } catch (NumberFormatException e) {
      LOG.error(String.format("Failed to parse string (%s) that was supposed to contain a port.",
          portString));
      throw e;
    }

    if (port < 0 || port > 0xFFFF) {
      throw new IllegalArgumentException(String.format("%d is not a valid port number.", port));
    }

    return port;
  }

  public static Map<String, AggregatedOffers> getAggregatedOffersPerNode(RotatingMap<Protos.OfferID, Protos.Offer> offers) {
    Map<String, AggregatedOffers> aggregatedOffersPerNode = new HashMap<>();

    for (Protos.Offer offer : offers.values()) {
      String hostName = offer.getHostname();

      AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(hostName);
      if (aggregatedOffers == null) {
        aggregatedOffers = new AggregatedOffers(offer);
        aggregatedOffersPerNode.put(hostName, aggregatedOffers);
      } else {
        aggregatedOffers.add(offer);
      }
    }

    for (AggregatedOffers aggregatedOffers : aggregatedOffersPerNode.values()) {
      LOG.info("Available resources at {}: {}", aggregatedOffers.getHostname(), aggregatedOffers.toString());
    }
    return aggregatedOffersPerNode;
  }

  public static int getSuicideTimeout(Map conf) {
    return Optional.fromNullable((Number) conf.get(SUICIDE_CONF))
        .or(DEFAULT_SUICIDE_TIMEOUT_SECS).intValue();
  }

  public static String getSupervisorStormLocalDir(Map conf) {
    return Optional.fromNullable((String) conf.get(SUPERVISOR_STORM_LOCAL_DIR_CONF))
        .or(DEFAULT_SUPERVISOR_STORM_LOCAL_DIR);
  }

  public static Map getFullTopologyConfig(Map conf, TopologyDetails info) {
    Map ret = new HashMap(conf);
    ret.putAll(info.getConf());
    return ret;
  }

  public static Number getScalarTopologyConf(Map conf, String configName, String topologyName, Number defaultValue) {
    Object configOption = conf.get(configName);

    Number ret = defaultValue;
    if (configOption != null && !(configOption instanceof Number)) {
      LOG.warn("Topology {} has invalid option \'{}\' -- Expected type \'{}\', Actual type \'{}\' -- Falling back to default of {}", topologyName, configName, Number.class, configOption.getClass(), defaultValue);
    } else if (configOption != null && configOption instanceof Number) {
      ret = (Number) configOption;
    } else {
      LOG.debug("Topology {} does not set {}, default of {} will be used", topologyName, configName, defaultValue);
    }
    return ret;
  }

  public static double topologyWorkerCpu(Map conf, TopologyDetails info) {
    Map topologyConfig = getFullTopologyConfig(conf, info);
    double topologyWorkerCpu = getScalarTopologyConf(topologyConfig, WORKER_CPU_CONF, info.getId(), DEFAULT_WORKER_CPU).doubleValue();
    if (topologyWorkerCpu < MESOS_MIN_CPU) {
      LOG.warn("Topology {} has invalid option \'{}\' -- {} is below {}, which is the minimum defined by Mesos -- Falling back to default of {}", info.getId(), WORKER_CPU_CONF, topologyWorkerCpu, MESOS_MIN_CPU, DEFAULT_WORKER_CPU);
      topologyWorkerCpu = DEFAULT_WORKER_CPU;
    }
    return topologyWorkerCpu;
  }

  public static double topologyWorkerMem(Map conf, TopologyDetails info) {
    Map topologyConfig = getFullTopologyConfig(conf, info);
    double topologyWorkerMem = getScalarTopologyConf(topologyConfig, WORKER_MEM_CONF, info.getId(), DEFAULT_WORKER_MEM_MB).doubleValue();
    if (topologyWorkerMem < MESOS_MIN_MEM_MB) {
      LOG.warn("Topology {} has invalid option \'{}\' -- {} is below {}, which is the minimum defined by Mesos -- Falling back to default of {}", info.getId(), WORKER_MEM_CONF, topologyWorkerMem, MESOS_MIN_MEM_MB, DEFAULT_WORKER_MEM_MB);
      topologyWorkerMem = DEFAULT_WORKER_MEM_MB;
    }
    return topologyWorkerMem;
  }

  public static double executorCpu(Map conf) {
    return Optional.fromNullable((Number) conf.get(EXECUTOR_CPU_CONF))
        .or(DEFAULT_EXECUTOR_CPU).doubleValue();
  }

  public static double executorMem(Map conf) {
    return Optional.fromNullable((Number) conf.get(EXECUTOR_MEM_CONF))
        .or(DEFAULT_EXECUTOR_MEM_MB).doubleValue();
  }

  public static String getRole(Map conf) {
    return Optional.fromNullable((String) conf.get(CONF_MESOS_ROLE)).or("*");
  }

}
