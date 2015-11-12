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

import com.google.common.base.Optional;
import org.apache.log4j.Logger;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandler;
import org.mortbay.jetty.handler.HandlerList;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;

import java.net.InetAddress;
import java.net.URI;

/**
 * Starts embedded Jetty server on random port to share a local directory.
 * Returns the full URL where files can be accessed.
 */

public class LocalFileServer {

  public static final Logger LOG = Logger.getLogger(LocalFileServer.class);
  private Server _server = new Server();

  public static void main(String[] args) throws Exception {

    URI url = new LocalFileServer().serveDir("/tmp2", "/tmp", Optional.<Integer>absent());
    System.out.println("************ " + url);

  }

  /**
   * Starts embedded Jetty server to share a local directory.
   * Returns the full URL where files can be retrieved.
   *
   * @param uriPath  - URI Path component e.g. /config
   * @param filePath - Directory path to be served. Can be absolute or relative. E.g. config
   * @return Full URI including server, port and path of baselevel dir. Please note that the ancient Jetty 6.1 Storm uses can't be configured to return directory listings AFAIK.
   * @throws Exception
   */
  public URI serveDir(String uriPath, String filePath, Optional<Integer> port) throws Exception {

    if (port.isPresent()) {
      LOG.info("Starting local file server on port: " + port.get());
      _server = new Server(port.get());
    } else {
      _server = new Server();
    }
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(0);
    _server.addConnector(connector);

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setResourceBase(filePath);

    ContextHandler staticContextHandler = new ContextHandler();
    staticContextHandler.setContextPath(uriPath);
    staticContextHandler.setHandler(resourceHandler);

    HandlerList handlers = new HandlerList();
    handlers.setHandlers(new Handler[]{staticContextHandler});
    _server.setHandler(handlers);
    _server.start();

    // get the connector once it is init so we can get the actual host & port it bound to.
    Connector initConn = _server.getConnectors()[0];
    return new URI("http", null, getHost(), initConn.getLocalPort(), uriPath, null, null);
  }

  private String getHost() throws Exception {
    return Optional.fromNullable((String) System.getenv("MESOS_NIMBUS_HOST"))
        .or(InetAddress.getLocalHost().getCanonicalHostName());
  }

  public void shutdown() throws Exception {
    _server.stop();
  }

}
