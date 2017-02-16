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

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class LaunchTaskTest
{

  /**
   * Testing target.
   */
  private final LaunchTask target;

  private final TaskInfo sampleTaskInfo;

  private final Offer sampleOffer;

  /**
   * Setup testing target & sample data.
   */
  public LaunchTaskTest()
  {
    SlaveID slaveID = SlaveID.newBuilder().setValue("s1").build();
    this.sampleTaskInfo =
        TaskInfo.newBuilder()
                .setName("t1").setSlaveId(slaveID)
                .setTaskId(TaskID.newBuilder().setValue("id2"))
                .setExecutor(
                    ExecutorInfo.newBuilder()
                                .setExecutorId(ExecutorID.newBuilder().setValue("e1"))
                                .setCommand(CommandInfo.getDefaultInstance()))
                .build();
    this.sampleOffer =
        Offer.newBuilder()
             .setHostname("h1").setSlaveId(slaveID)
             .setId(OfferID.newBuilder().setValue("id1"))
             .setFrameworkId(FrameworkID.newBuilder().setValue("f1").build())
             .build();
    this.target = new LaunchTask(sampleTaskInfo, sampleOffer);
  }

  @Test
  public void getTask()
  {
    assertSame(
        "The return value should be the same as the value passed in the argument of the constructor",
        sampleTaskInfo, target.getTask()
    );
  }

  @Test
  public void getOffer()
  {
    assertSame(
        "The return value should be the same as the value passed in the argument of the constructor",
        sampleOffer, target.getOffer()
    );
  }

  @Test
  public void objectToString()
  {
    assertEquals(
        "Offer: {\"offer_id\":\"id1\",\"hostname\":\"h1\"} TaskInfo: {\"task_id\":\"id2\",\"slave_id\":\"s1\",\"executor_id\":\"e1\"}",
        target.toString()
    );
  }

}
