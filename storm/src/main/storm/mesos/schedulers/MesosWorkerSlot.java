package storm.mesos.schedulers;

import backtype.storm.scheduler.WorkerSlot;

/**
 * Created by ksoundararaj on 2/13/16.
 */

public class MesosWorkerSlot extends WorkerSlot {
  private String topologyId;

  public MesosWorkerSlot(String nodeId, Number port, String topologyId) {
    super(nodeId, port);
    this.topologyId = topologyId;
  }

  public String getTopologyId() {
    return this.topologyId;
  }
}
