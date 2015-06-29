package storm.mesos.logviewer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import storm.mesos.MesosCommon;
import com.google.common.base.Optional;

public class LogViewerController implements ILogController {
    private static final Logger LOG = Logger.getLogger(LogViewerController.class);
    protected Process process;
    protected IUrlDetection urlDetector;
    protected String installLocation;
    protected Map<String, Object> config;
    protected Optional<Integer> port;
    protected static final String LOG_VIEWER_PORT = "logviewer.port";
    
    /**
     * Create a log view controller with a SocketUrlDetector set to 
     * the port that is in the storm.yaml config file. If no port is
     * set then use the default of 8000. 
     */
    public LogViewerController() {
        // Use a simplistic approach.
        this(new SocketUrlDetection());
    }
    
    /**
     * Load a preconfigured Url detector to check for an existing logviewer.
     * 
     * @param urlDetector
     */
    public LogViewerController(IUrlDetection urlDetector) {
        setUrlDetector(urlDetector);
        initialize();
    }
    
    /**
     * Start up the logviewer, but before that is done a check is made to
     * see if an existing logviewer (or process) is on the same port and report
     * an error if so.
     */
    @Override
    public void start() {
       try {
           if (!exists()){
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
    
    public void setUrlDetector(IUrlDetection urlDetector) {
        this.urlDetector = urlDetector;
    }
    
    public IUrlDetection getUrlDetector() {
        return urlDetector;
    }
    
    public void setInstallLocation(String location) {
        installLocation = location;
    }
    
    public String getInstallLocation() {
        return installLocation;
    }

    protected void setProcess(Process process) {
        this.process = process;
    }
    
    protected Process getProcess() {
        return process;
    }
    
    protected void setPort(Optional<Integer> oport) {
        port = oport;
    }
    
    protected Optional<Integer> getPort() {
        return port;
    }
    
    protected void setConfig(Map<String, Object> configuration) {
        config = configuration;
    }
    
    protected Map<String, Object> getConfig(){
        return config;
    }

    protected void launchLogViewer () throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("launchLogViewer: home dir? " + getInstallLocation());
        }
        ProcessBuilder pb = createProcessBuilder(getInstallLocation());
        setProcess(pb.start());       
    }
    
    /**
     * Create a process builder to launch the log viewer
     * @param logDirectory
     * @return
     */
    protected ProcessBuilder createProcessBuilder(String homeDirectory) {
        ProcessBuilder pb = new ProcessBuilder("nohup", "/usr/bin/python", "-u", Paths.get(homeDirectory, "/bin/storm").toString(), "logviewer");

        // This means to look in a common area for the worker logs.
        // TODO: Either start multiple logviewers or only one Supervisor.
        if (config.containsKey(MesosCommon.WORKER_LOG_DIR)) {
            String logDir = (String) config.get(MesosCommon.WORKER_LOG_DIR);
            LOG.info("Logviewer will look for worker logs in " + logDir);
            pb.environment().put("STORM_LOG_DIR", logDir);
        }
        // If anything goes wrong at startup we want to see it.
        File log = Paths.get(homeDirectory, "/logs/logviewer-startup.log").toFile();
        pb.redirectErrorStream(true);
        pb.redirectOutput(Redirect.appendTo(log)); 
        return pb;
    }
    
    @SuppressWarnings("unchecked")
    protected void initialize() {
        setInstallLocation(new File(".").getAbsolutePath());
        
        loadConfig();
        
        // If the storm configuration has defined a port use it.
        if (getPort().isPresent()) {
            getUrlDetector().setPort(getPort());
        } else {
            // Otherwise use the default 
            LOG.info("No valid port in storm.yaml, using the on configured in the Url Detector " + 
                getUrlDetector().getPort());
            setPort(getUrlDetector().getPort());
        }
    }
    
    @SuppressWarnings("unchecked")
    protected void loadConfig() {
        InputStream input = null;
        try {
            input = new FileInputStream(Paths.get(getInstallLocation(), "/conf/storm.yaml").toFile());
            setConfig((Map<String, Object>) new Yaml().load(input));
            LOG.info("Setting port for log viewer to " + config.get(LOG_VIEWER_PORT));
            setPort(Optional.of((Integer) getConfig().get(LOG_VIEWER_PORT)));
        } catch (Exception e) {
            LOG.error("Failed to configure the log viewer controller,", e);
            setPort(Optional.fromNullable(((Integer) null)));
        } finally {
            IOUtils.closeQuietly(input);
        }
    }
}