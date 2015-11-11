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
package storm.mesos.logviewer;

import backtype.storm.Config;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Paths;
import java.util.Map;

public class LogViewerController implements ILogController {
  private static final Logger LOG = Logger.getLogger(LogViewerController.class);
  protected Process process;
  protected IUrlDetection urlDetector;
  protected Integer port;

  public LogViewerController(Map conf) {
    Number port = (Number) conf.get(Config.LOGVIEWER_PORT);
    if (port == null) {
      port = 8000;
    }
    this.port = port.intValue();
    setUrlDetector(new SocketUrlDetection(this.port));
  }

  /**
   * Start up the logviewer, but before that is done a check is made to
   * see if an existing logviewer (or process) is on the same port and report
   * and error if so.
   */
  @Override
  public void start() {
    try {
      if (!exists()) {
        launchLogViewer();
      } else {
        LOG.error("Failed to start logviewer because there is something on its port");
      }
    } catch (Exception e) {
      LOG.error("Failed to start logviewer", e);
    }
  }

  @Override
  public void stop() {
    getProcess().destroy();
  }

  @Override
  public boolean exists() {
    return getUrlDetector().isReachable();
  }

  public IUrlDetection getUrlDetector() {
    return urlDetector;
  }

  public void setUrlDetector(IUrlDetection urlDetector) {
    this.urlDetector = urlDetector;
  }

  protected Process getProcess() {
    return process;
  }

  protected void setProcess(Process process) {
    this.process = process;
  }

  protected void launchLogViewer() throws IOException {
    ProcessBuilder pb = createProcessBuilder();
    setProcess(pb.start());
  }

  /**
   * Create a process builder to launch the log viewer
   * @param logDirectory
   * @return
   */
  protected ProcessBuilder createProcessBuilder() {
    ProcessBuilder pb = new ProcessBuilder(
        Paths.get(System.getProperty("user.dir"), "/bin/storm").toString(),
        "logviewer",
        "-c",
        "storm.log.dir=" + System.getenv("MESOS_SANDBOX") + "/logs",
        "-c",
        Config.LOGVIEWER_PORT + "=" + port
    );

    // If anything goes wrong at startup we want to see it.
    Paths.get(System.getenv("MESOS_SANDBOX"), "/logs").toFile().mkdirs();
    File log = Paths.get(System.getenv("MESOS_SANDBOX"), "/logs/logviewer-startup.log").toFile();
    pb.redirectErrorStream(true);
    pb.redirectOutput(Redirect.appendTo(log));
    return pb;
  }
}
