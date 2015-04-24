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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.json.simple.JSONValue;


/**
 * This utility class provides methods to improve logging of Mesos protobuf objects.
 * These methods don't perform any logging directly, instead they return Strings
 * which can be logged by callers.
 *
 * The methods offer more concise and readable String representations of protobuf
 * objects than you get by calling a protobuf object's native toString method.
 *
 * Another advantage over standard protobuf toString() output is that this output is
 * in proper JSON format, resulting in logs that can be more easily parsed.
 *
 * TODO(erikdw):
 *  1. Check whether a value is set/null when deciding whether to include it.
 *  2. Currently we only include the object values that we care about for a
 *     storm-only mesos-cluster.  We should instead allow configuration for choosing
 *     which fields to include (e.g., a bitmap toggling certain fields on/off).
 *  3. Allow cleanly logging to separate Logger, to allow configuring the logs going to
 *     a separate log file.  The complication is that these methods lack context,
 *     they are just pretty-printing the protobuf objects.
 */
public class PrettyProtobuf {

  /**
   * Ideally we'd have a generic method for these getTrimmedString methods, but
   * these protobuf types don't implement a common interface (which would need
   * to include getValue()).
   */

  /**
   * Pretty-print the mesos protobuf OfferID.
   */
  public static String getTrimmedString(OfferID id) {
    return id.getValue().toString().trim();
  }

  /**
   * Pretty-print the mesos protobuf SlaveID.
   */
  public static String getTrimmedString(SlaveID id) {
    return id.getValue().toString().trim();
  }

  /**
   * Pretty-print the mesos protobuf ExecutorID.
   */
  public static String getTrimmedString(ExecutorID id) {
    return id.getValue().toString().trim();
  }

  /**
   * Pretty-print the mesos protobuf TaskID.
   */
  public static String getTrimmedString(TaskID id) {
    return id.getValue().toString().trim();
  }

  /**
   * Pretty-print the mesos protobuf TaskState.
   */
  public static String getTrimmedString(TaskState state) {
    return state.toString().trim();
  }

  /**
   * Pretty-print mesos protobuf TaskStatus.
   */
  public static String taskStatusToString(TaskStatus taskStatus) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("task_id", getTrimmedString(taskStatus.getTaskId()));
    map.put("slave_id", getTrimmedString(taskStatus.getSlaveId()));
    map.put("state", getTrimmedString(taskStatus.getState()));
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print mesos protobuf TaskInfo.
   *
   * XXX(erikdw): not including command, container (+data), nor health_check.
   */
  public static String taskInfoToString(TaskInfo task) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("task_id", getTrimmedString(task.getTaskId()));
    map.put("slave_id", getTrimmedString(task.getSlaveId()));
    map.putAll(resourcesToOrderedMap(task.getResourcesList()));
    map.put("executor_id", getTrimmedString(task.getExecutor().getExecutorId()));
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print mesos protobuf Offer.
   *
   * XXX(erikdw): not including slave_id, attributes, executor_ids, nor framework_id.
   */
  public static String offerToString(Offer offer) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("offer_id", getTrimmedString(offer.getId()));
    map.put("hostname", offer.getHostname());
    map.putAll(resourcesToOrderedMap(offer.getResourcesList()));
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print List of mesos protobuf Offers.
   */
  public static String offerListToString(List<Offer> offers) {
    List<String> offersAsStrings = Lists.transform(offers, offerToStringTransform);
    return "[\n" + StringUtils.join(offersAsStrings, ",\n") + "]";
  }

  /**
   * Pretty-print List of mesos protobuf TaskInfos.
   */
  public static String taskInfoListToString(List<TaskInfo> tasks) {
    List<String> tasksAsStrings = Lists.transform(tasks, taskInfoToStringTransform);
    return "[\n" + StringUtils.join(tasksAsStrings, ",\n") + "]";
  }

  /**
   * Pretty-print the values in the Offer map used in MesosNimbus.
   *
   * Callers must ensure they have locked the Map first, else they could
   * have inconsistent output since the _offers map is touched from both
   * mesos-driven events and storm-driven calls.
   *
   * FIXME(erikdw): figure out a design better that removes the need
   * for external callers to lock before calling this method.
   */
  public static String offerMapToString(RotatingMap<OfferID, Offer> offers) {
    List<String> offersAsStrings = Lists.transform(new ArrayList<Offer>(offers.values()),
        offerToStringTransform);
    return "[\n" + StringUtils.join(offersAsStrings, ",\n") + "]";
  }

  /**
   * Wrapper around offerToString which allows using gauva's transform utility.
   */
  private static Function<Offer, String> offerToStringTransform =
      new Function<Offer,String>() {
        public String apply(Offer o) { return offerToString(o); }
      };

  /**
   * Wrapper around taskInfoToString which allows using gauva's transform utility.
   */
  private static Function<TaskInfo, String> taskInfoToStringTransform =
      new Function<TaskInfo,String>() {
        public String apply(TaskInfo t) { return taskInfoToString(t); }
      };

  /**
   * Wrapper around rangeToString which allows using gauva's transform utility.
   */
  private static Function<Range, String> rangeToStringTransform =
      new Function<Range,String>() {
        public String apply(Range r) { return rangeToString(r); }
      };

  /**
   * Create String representation of mesos protobuf Range type.
   */
  private static String rangeToString(Range range) {
    String beginStr = String.valueOf(range.getBegin());
    String endStr = String.valueOf(range.getEnd());
    /*
     * A Range representing a single number still has both Range.begin
     * and Range.end populated, but they are set to the same value.
     * In that case we just return "N" instead of "N-N".
     */
    if (range.getBegin() == range.getEnd()) {
      return beginStr;
    } else {
      return beginStr + "-" + endStr;
    }
  }

  /**
   * Pretty-print List of mesos protobuf Ranges.
   */
  private static String rangeListToString(List<Range> ranges) {
    List<String> rangesAsStrings = Lists.transform(ranges, rangeToStringTransform);
    return "[" + StringUtils.join(rangesAsStrings, ",") + "]";
  }

  /**
   * Construct a Map of Resource names to String values.
   * Ensure the order is always the same (cpu, mem, then ports), so that
   * we have consistently ordered log output.
   */
  private static Map<String, String> resourcesToOrderedMap(List<Resource> resources) {
    String cpus = null, mem = null, ports = null;
    for (Resource r : resources) {
      switch (r.getName()) {
        case "cpus":
          cpus = String.valueOf(r.getScalar().getValue());
          break;
        case "mem":
          mem = String.valueOf(r.getScalar().getValue());
          break;
        case "ports":
          ports = rangeListToString(r.getRanges().getRangeList());
          break;
      }
    }
    Map<String, String> map = new LinkedHashMap<>();
    if (cpus != null) {
      map.put("cpus", cpus);
    }
    if (mem != null) {
      map.put("mem", mem);
    }
    if (ports != null) {
      map.put("ports", ports);
    }
    return map;
  }

}
