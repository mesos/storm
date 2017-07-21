package storm.mesos;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import storm.mesos.util.ZKClient;
import org.apache.curator.test.TestingServer;

/**
 * Created by rtang on 7/21/17.
 */
public class ZKClientTest {
  /**
   * Testing target.
   */
  private final ZKClient target;

  /**
   * Setup testing target & sample data.
   */
  public ZKClientTest() {
    String connString = "localhost";
    try {
      TestingServer server = new TestingServer(true);
      connString = server.getConnectString();
    } catch (Exception e) {
      assertTrue("Couldn't create test server", false);
    }
    target = new ZKClient(connString);
  }

  @Test
  public void testCreateNodeThenDelete() {
    boolean success = false;
    String pathName = "/test1";

    success = target.createNode(pathName);
    assertTrue("Couldn't create node", success);

    target.deleteNode(pathName);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(pathName));
  }

  @Test
  public void testCreateNodeCheckExistenceThenDelete() {
    boolean success = false;
    String pathName = "/test2";

    success = target.createNode(pathName);
    assertTrue("Couldn't create node", success);

    assertTrue("Node created but doesn't exist", target.nodeExists(pathName));

    target.deleteNode(pathName);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(pathName));
  }

  @Test
  public void testCheckExistenceOfNonexistentNode() {
    String pathName = "/test3";
    assertFalse("Nonexistent node exists for some reason", target.nodeExists(pathName));
  }

  @Test
  public void testCreateNodeGetDataThenDelete() {
    boolean success = false;
    String pathName = "/test4";
    String initialString = "test";

    success = target.createNode(pathName, initialString);
    assertTrue("Couldn't create node", success);

    String returnedString = target.getNodeData(pathName);
    assertTrue("Data retrieved doesn't match initial data", initialString.equals(returnedString));

    target.deleteNode(pathName);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(pathName));
  }

  @Test
  public void testCreateNodeUpdateDataThenDelete() {
    boolean success = false;
    String pathName = "/test5";
    String initialString = "test";

    success = target.createNode(pathName, initialString);
    assertTrue("Couldn't create node", success);

    String updatedString = "updated";
    success = target.updateNodeData(pathName, updatedString);
    assertTrue("Couldn't update node data", success);

    String returnedString = target.getNodeData(pathName);
    assertTrue("Data retrieved doesn't match updated data", updatedString.equals(returnedString));

    target.deleteNode(pathName);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(pathName));
  }

  @After
  public void closeConnection() {
    target.close();
  }
}
