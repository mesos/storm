package storm.mesos.logviewer;

import com.google.common.base.Optional;

public interface IUrlDetection {

    public boolean isReachable();
    
    public Optional<Integer> getPort();
    
    public void setPort(Optional<Integer> port);
}
