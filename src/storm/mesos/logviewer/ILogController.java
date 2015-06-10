package storm.mesos.logviewer;

public interface ILogController {

    public void start();
    
    public void stop();
    
    public boolean exists();
    
}
