package storm.mesos.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ZKClient allows you to interact with ZooKeeper. Primarily used for tracking logviewer state on hosts thus far but
 * can be used to track any metadata needed in the future.
 */

public class ZKClient {
  CuratorFramework _client;
  public static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
  private static final int BASE_SLEEP_TIME_MS = 1000;
  private static final int MAX_RETRIES = 3;

  public ZKClient() {
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
    _client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
    _client.start();
  }

  public ZKClient(String connectionString) {
    LOG.info("Attempting to connect to following ZooKeeper servers: {}", connectionString);
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
    _client = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    _client.start();
  }

  public ZKClient(String connectionString, int connectionTimeout, int sessionTimeout) {
    LOG.info("Attempting to connect to following ZooKeeper servers: {}", connectionString);
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(BASE_SLEEP_TIME_MS, MAX_RETRIES);
    _client = CuratorFrameworkFactory.builder()
                                     .connectString(connectionString)
                                     .retryPolicy(retryPolicy)
                                     .connectionTimeoutMs(connectionTimeout)
                                     .sessionTimeoutMs(sessionTimeout)
                                     .build();
    _client.start();
  }

  public boolean createNode(String path, String data) {
    try {
      _client.create().creatingParentsIfNeeded().forPath(path, data.getBytes());
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
      return false;
    }
  }

  public boolean createNode(String path) {
    try {
      // default payload of byte[0]
      _client.create().creatingParentsIfNeeded().forPath(path);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
      return false;
    }
  }

  public void deleteNode(String path) {
    try {
      // the delete is guaranteed even if initial failure, curator will attempt to delete in background until successful
      _client.delete().guaranteed().forPath(path);
    } catch (Exception e) {
      // swallow exception because delete is guaranteed
    }
  }

  public boolean updateNodeData(String path, String data) {
    try {
      _client.setData().forPath(path, data.getBytes());
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
      return false;
    }
  }

  public boolean updateNodeData(String path, byte[] data) {
    try {
      _client.setData().forPath(path, data);
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
      return false;
    }
  }

  public byte[] getNodeData(String path) {
    byte[] rawData = null;
    try {
      rawData = _client.getData().forPath(path);
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return rawData;
  }

  public boolean nodeExists(String path) {
    try {
      Stat stat = _client.checkExists().forPath(path);
      if (stat == null) {
        return false;
      }
      return true;
    } catch (Exception e) {
      LOG.error(e.toString());
      return false;
    }
  }

  public List<String> getChildren(String path) {
    List<String> children = null;
    try {
      children = _client.getChildren().forPath(path);
    } catch (Exception e) {
      LOG.error(e.toString());
    }
    return children;
  }

  public void close() {
    _client.close();
  }
}
