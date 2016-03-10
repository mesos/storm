package storm.mesos.schedulers;


import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.WorkerSlot;
import org.apache.mesos.Protos;
import storm.mesos.util.RotatingMap;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * A scheduler needs to implement the following interface for it to be MesosNimbus compatible.
 */
public interface IMesosStormScheduler {

  /**
   * This method is invoked by Nimbus when it wants to get a list of worker slots that are available for assigning the
   * topology workers. In Nimbus's view, a "WorkerSlot" is a host and port that it can use to assign a worker.
   * <p/>
   * TODO: Rotating Map itself needs to be refactored. Perhaps make RotatingMap inherit Map so users can pass in a
   * map? or Make the IMesosStormScheduler itself generic so _offers could be of any type?
   */
  public List<WorkerSlot> allSlotsAvailableForScheduling(RotatingMap<Protos.OfferID, Protos.Offer> offers,
                                                         Collection<SupervisorDetails> existingSupervisors,
                                                         Topologies topologies, Set<String> topologiesMissingAssignments);
}
