package storm.mesos;

import java.net.InetAddress;
import java.net.URI;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.*;
import org.apache.log4j.Logger;
import org.mortbay.jetty.nio.SelectChannelConnector;

import javax.servlet.ServletContext;

/**
 * Starts embedded Jetty server on random port to share a local directory.
 * Returns the full URL where files can be accessed.
 **/

public class LocalFileServer {

    public static final Logger LOG = Logger.getLogger(LocalFileServer.class);
    private Server _server = new Server();


    /**
     * Starts embedded Jetty server on random port to share a local directory.
     * Returns the full URL where files can be retrieved.
     *
     * @param uriPath - URI Path component e.g. /config
     * @param filePath - Directory path to be served. Can be absolute or relative. E.g. config
     * @return Full URI including server, port and path of baselevel dir. Please note that the ancient Jetty 6.1 Storm uses can't be configured to return directory listings AFAIK.
     * @throws Exception
     */
    public URI serveDir(String uriPath, String filePath) throws Exception {

        _server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(0);
        _server.addConnector(connector);

        ResourceHandler resource_handler = new ResourceHandler();
        resource_handler.setResourceBase(filePath);

        ContextHandler staticContextHandler = new ContextHandler();
        staticContextHandler.setContextPath(uriPath);
        staticContextHandler.setHandler(resource_handler);

        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { staticContextHandler});
        _server.setHandler(handlers);
        _server.start();

        // get the connector once it is init so we can get the actual host & port it bound to.
        Connector initConn = _server.getConnectors()[0];
        return new URI("http", null, getHost(), initConn.getLocalPort(), uriPath, null, null);
    }

    private String getHost() throws Exception {
        final String envHost = System.getenv("MESOS_NIMBUS_HOST");
        if (envHost == null) {
            return InetAddress.getLocalHost().getHostName();
        } else {
            return envHost;
        }
    }

    public void shutdown() throws Exception {
        _server.stop();
    }

    public static void main(String[] args) throws Exception {

        URI url = new LocalFileServer().serveDir("/tmp2","/tmp");
        System.out.println("************ " + url);

    }

}
