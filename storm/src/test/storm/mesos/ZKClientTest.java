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
      assertTrue("Couldn't not create test server", false);
    }
    target = new ZKClient(connString);
  }

  @Test
  public void testCreateNodeThenDelete() {
    boolean success = false;

    success = target.createNode("/test1");
    if (!success) assertTrue("Couldn't create node", false);

    target.deleteNode("/test1");
  }

  @Test
  public void testCreateNodeCheckExistenceThenDelete() {
    boolean success = false;

    success = target.createNode("/test2");
    if (!success) assertTrue("Couldn't create node", false);

    assertTrue("Node created but doesn't exist", target.nodeExists("/test2"));

    target.deleteNode("/test2");
  }

  @Test
  public void testCheckExistenceOfNonexistentNode() {
      assertFalse("Nonexistent node exists for some reason", target.nodeExists("/test3"));
  }

  @Test
  public void testCreateNodeGetDataThenDelete() {
    boolean success = false;
    String initialString = "test";

    success = target.createNode("/test4", initialString);
    if (!success) assertTrue("Couldn't create node", false);

    String returnedString = target.getNodeData("/test4");
    assertTrue("Data retrieved doesn't match initial data", initialString.equals(returnedString));

    target.deleteNode("/test4");
  }

  @Test
  public void testCreateNodeUpdateDataThenDelete() {
    boolean success = false;
    String initialString = "test";

    success = target.createNode("/test5", initialString);
    if (!success) assertTrue("Couldn't create node", false);

    String updatedString = "updated";
    success = target.updateNodeData("/test5", updatedString);
    if (!success) assertTrue("Couldn't update node data", false);

    String returnedString = target.getNodeData("/test5");
    assertTrue("Data retrieved doesn't match updated data", updatedString.equals(returnedString));

    target.deleteNode("/test5");
  }

  @After
  public void closeConnection() {
    target.close();
  }
}
