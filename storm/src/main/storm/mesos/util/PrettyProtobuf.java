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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Set;
import org.json.simple.JSONValue;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 * This desire to print in JSON is why we populate Maps below for each multi-field
 * object. Also, we use LinkedHashMap and TreeMap to ensure the order of the fields
 * in multi-field objects stays consistent. That allows log lines to be visually
 * compared whilst examining the state of the system.
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
   * Pretty-print mesos protobuf TaskStatus.
   */
  public static String taskStatusToString(TaskStatus taskStatus) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("task_id", taskStatus.getTaskId().getValue());
    map.put("slave_id", taskStatus.getSlaveId().getValue());
    map.put("state", taskStatus.getState().toString());
    if (taskStatus.hasMessage()) {
      map.put("message", taskStatus.getMessage());
    }
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print mesos protobuf TaskInfo.
   * <p/>
   * XXX(erikdw): not including command, container (+data), nor health_check.
   */
  public static String taskInfoToString(TaskInfo task) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("task_id", task.getTaskId().getValue());
    map.put("slave_id", task.getSlaveId().getValue());
    map.putAll(resourcesToOrderedMap(task.getResourcesList()));
    map.put("executor_id", task.getExecutor().getExecutorId().getValue());
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print mesos protobuf Offer.
   * <p/>
   * XXX(erikdw): not including slave_id, attributes, executor_ids, nor framework_id.
   */
  public static String offerToString(Offer offer) {
    Map<String, String> map = new LinkedHashMap<>();
    map.put("offer_id", offer.getId().getValue());
    map.put("hostname", offer.getHostname());
    map.putAll(resourcesToOrderedMap(offer.getResourcesList()));
    return JSONValue.toJSONString(map);
  }

  /**
   * Pretty-print List of mesos protobuf Offers.
   */
  public static String offerListToString(List<Offer> offers) {
    List<String> offersAsStrings = Lists.transform(offers, offerToStringTransform);
    return String.format("[\n%s]", StringUtils.join(offersAsStrings, ",\n"));
  }

  /**
   * Pretty-print List of mesos protobuf TaskInfos.
   */
  public static String taskInfoListToString(List<TaskInfo> tasks) {
    List<String> tasksAsStrings = Lists.transform(tasks, taskInfoToStringTransform);
    return String.format("[%s]", StringUtils.join(tasksAsStrings, ", "));
  }

  /**
   * Pretty-print the values in the Offer map used in MesosNimbus.
   * <p/>
   * Callers must ensure they have locked the Map first, else they could
   * have inconsistent output since the _offers map is touched from both
   * mesos-driven events and storm-driven calls.
   * <p/>
   * TODO:(erikdw): figure out a design better that removes the need
   * for external callers to lock before calling this method.
   */
  public static String offerMapToString(Map<OfferID, Offer> offers) {
    List<String> offersAsStrings = Lists.transform(new ArrayList<Offer>(offers.values()),
                                                   offerToStringTransform);
    return String.format("[\n%s]", StringUtils.join(offersAsStrings, ",\n"));
  }

  /**
   * Wrapper around offerToString which allows using gauva's transform utility.
   */
  private static Function<Offer, String> offerToStringTransform =
      new Function<Offer, String>() {
        public String apply(Offer o) {
          return offerToString(o);
        }
      };

  /**
   * Wrapper around taskInfoToString which allows using gauva's transform utility.
   */
  private static Function<TaskInfo, String> taskInfoToStringTransform =
      new Function<TaskInfo, String>() {
        public String apply(TaskInfo t) {
          return taskInfoToString(t);
        }
      };

  /**
   * Wrapper around rangeToString which allows using gauva's transform utility.
   */
  private static Function<Range, String> rangeToStringTransform =
      new Function<Range, String>() {
        public String apply(Range r) {
          return rangeToString(r);
        }
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
      return String.format("%s-%s", beginStr, endStr);
    }
  }

  /**
   * Pretty-print mesos protobuf Ranges.
   */
  private static String rangesToString(Ranges ranges) {
    List<String> rangesAsStrings = Lists.transform(ranges.getRangeList(), rangeToStringTransform);
    return String.format("[%s]", StringUtils.join(rangesAsStrings, ","));
  }

  /**
   * Pretty-print mesos protobuf Set.
   */
  private static String setToString(Set set) {
    return String.format("[%s]", StringUtils.join(set.getItemList(), ","));
  }

  /**
   * Return Resource names mapped to values.
   */
  private static Map<String, String> resourcesToOrderedMap(List<Resource> resources) {
    Map<String, String> map = new TreeMap<>();
    for (Resource r : resources) {
      String name;
      String value = "";
      if (r.hasRole()) {
        name = String.format("%s(%s)", r.getName(), r.getRole());
      } else {
        name = r.getName();
      }
      switch (r.getType()) {
        case SCALAR:
          value = String.valueOf(r.getScalar().getValue());
          break;
        case RANGES:
          value = rangesToString(r.getRanges());
          break;
        case SET:
          value = setToString(r.getSet());
          break;
        default:
          // If hit, then a new Resource Type needs to be handled here.
          value = String.format("Unrecognized Resource Type: `%s'", r.getType());
          break;
      }
      map.put(name, value);
    }
    return map;
  }

  /**
   * Wrapper around getTrimmedString which allows using gauva's transform utility.
   */
  private static Function<OfferID, String> offerIDToStringTransform =
      new Function<OfferID, String>() {
        public String apply(OfferID o) {
          return o.getValue().toString();
        }
      };

  public static String offerIDListToString(List<OfferID> offerIDList) {
    List<String> offerIDsAsStrings = Lists.transform(offerIDList, offerIDToStringTransform);
    return String.format("[%s]", StringUtils.join(offerIDsAsStrings, ", "));
  }
}
