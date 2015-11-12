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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class SocketUrlDetection {
  private static final Logger LOG = Logger.getLogger(SocketUrlDetection.class);
  protected Integer port;

  public SocketUrlDetection(Integer port) {
    this.port = port;
  }

  public boolean isReachable() {
    Socket socket = null;
    boolean reachable = false;
    try {
      LOG.info("Checking host " + InetAddress.getLocalHost() + " and port " + getPort());

      socket = new Socket(InetAddress.getLocalHost(), getPort());
      reachable = true;
    } catch (IOException e) {
      // don't care.
      LOG.warn(e);
    } finally {
      IOUtils.closeQuietly(socket);
    }
    return reachable;
  }

  public Integer getPort() {
    return port;
  }
}
