package storm.mesos;

import backtype.storm.scheduler.TopologyDetails;
import org.apache.log4j.Logger;

import java.util.*;

public class MesosCommon {
  public static final Logger LOG = Logger.getLogger(MesosCommon.class);

  public static final String CPU_CONF = "topology.mesos.worker.cpu";
  public static final String MEM_CONF = "topology.mesos.worker.mem.mb";
  public static final String SUICIDE_CONF = "mesos.supervisor.suicide.inactive.timeout.secs";

  public static final double DEFAULT_CPU = 1;
  public static final double DEFAULT_MEM_MB = 1000;
  public static final int DEFAULT_SUICIDE_TIMEOUT_SECS = 120;

  public static final String SUPERVISOR_ID = "supervisorid";
  public static final String ASSIGNMENT_ID = "assignmentid";

  public static String taskId(String nodeid, int port) {
    return nodeid + "-" + port;
  }

  public static int portFromTaskId(String taskId) {
    int last = taskId.lastIndexOf("-");
    String port = taskId.substring(last + 1);
    return Integer.parseInt(port);
  }

  public static int getSuicideTimeout(Map conf) {
    Number timeout = (Number) conf.get(SUICIDE_CONF);
    if (timeout == null) return DEFAULT_SUICIDE_TIMEOUT_SECS;
    else return timeout.intValue();
  }

  public static Map getFullTopologyConfig(Map conf, TopologyDetails info) {
    Map ret = new HashMap(conf);
    ret.putAll(info.getConf());
    return ret;
  }

  public static double topologyCpu(Map conf, TopologyDetails info) {
    conf = getFullTopologyConfig(conf, info);
    Object cpuObj = conf.get(CPU_CONF);
    if (cpuObj != null && !(cpuObj instanceof Number)) {
      LOG.warn("Topology has invalid mesos cpu configuration: " + cpuObj + " for topology " + info.getId());
      cpuObj = null;
    }
    if (cpuObj == null) return DEFAULT_CPU;
    else return ((Number) cpuObj).doubleValue();
  }

  public static double topologyMem(Map conf, TopologyDetails info) {
    conf = getFullTopologyConfig(conf, info);
    Object memObj = conf.get(MEM_CONF);
    if (memObj != null && !(memObj instanceof Number)) {
      LOG.warn("Topology has invalid mesos mem configuration: " + memObj + " for topology " + info.getId());
      memObj = null;
    }
    if (memObj == null) return DEFAULT_MEM_MB;
    else return ((Number) memObj).doubleValue();
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
