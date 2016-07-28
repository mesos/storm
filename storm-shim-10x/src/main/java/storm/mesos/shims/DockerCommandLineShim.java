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
package storm.mesos.shims;

import storm.mesos.util.MesosCommon;

import java.util.Map;

public class DockerCommandLineShim implements ICommandLineShim {
  Map stormConf;
  String extraConfig;

  public DockerCommandLineShim(Map stormConf, String extraConfig) {
    this.stormConf = stormConf;
    this.extraConfig = extraConfig;
  }

  public String getCommandLine(String topologyId) {
    // An ugly workaround for a bug in DCOS
    Map<String, String> env = System.getenv();
    String javaLibPath = env.get("MESOS_NATIVE_JAVA_LIBRARY");
    return String.format(
        "export MESOS_NATIVE_JAVA_LIBRARY=%s" +
        " && export STORM_SUPERVISOR_LOG_FILE=%s-supervisor.log" +
        " && /bin/cp $MESOS_SANDBOX/storm.yaml conf " +
        " && /usr/bin/python bin/storm.py supervisor storm.mesos.MesosSupervisor " +
        "-c storm.log.dir=%s%s",
        javaLibPath,
        topologyId,
        MesosCommon.getStormLogDir(stormConf, "$MESOS_SANDBOX/logs"),
        extraConfig);
  }

}
