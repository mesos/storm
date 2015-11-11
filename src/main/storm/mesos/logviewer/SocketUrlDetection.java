package storm.mesos.logviewer;

import java.net.InetAddress;
import java.net.Socket;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

public class SocketUrlDetection implements IUrlDetection {
    public static final int DEFAULT_PORT = 8000;
    protected Optional<Integer> port;
    private static final Logger LOG = Logger.getLogger(SocketUrlDetection.class);
    
    public SocketUrlDetection() {
        setPort(Optional.of(DEFAULT_PORT));
    }
    
    public SocketUrlDetection(int port) {
        setPort(Optional.of(port));
    }
    
    public SocketUrlDetection(Optional<Integer> port) {
        setPort(port);
    }
    
    @Override
    public boolean isReachable() {
        Socket socket = null;
        boolean reachable = false;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Checking host " + InetAddress.getLocalHost() + " and port " + getPort());
            }
            
            if (port.isPresent()) {
                socket = new Socket(InetAddress.getLocalHost(), getPort().get());
                reachable = true;
            }
        } catch (Exception e) {
            // don't care.
        } finally {
            IOUtils.closeQuietly(socket);
        }
        return reachable;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void setPort(Optional port) {
        this.port = (Optional<Integer>) port;
    }
    
    @Override
    public Optional<Integer> getPort() {
        return port;
    }
}
