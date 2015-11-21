package storm.mesos.shims;

import java.util.Map;

/**
 * Wraps LocalState
 */
public interface ILocalStateShim {

  public String get(String key);

  public void put(String key, String value);

  public void remove(String key);

  public Map<String, String> snapshot();

}
