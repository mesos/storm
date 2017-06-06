package storm.mesos;

import org.apache.mesos.Protos;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import storm.mesos.util.ZKClient;
import org.apache.curator.test.TestingServer;

import java.util.List;

public class ZKClientTest {
  /**
   * Testing target.
   */
  private final ZKClient target;

  /**
   * Setup testing target & sample data.
   */
  public ZKClientTest() {
    String connString = "localhost:2181";
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

    String returnedString = new String(target.getNodeData(pathName));
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

    String returnedString = new String(target.getNodeData(pathName));
    assertTrue("Data retrieved doesn't match updated data", updatedString.equals(returnedString));

    target.deleteNode(pathName);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(pathName));
  }

  @Test
  public void testCreateNestedNodeThenDelete() {
    boolean success = false;
    String parentPath = "/parent";
    String nestedPathOne = "/parent/childOne";
    String nestedPathTwo = "/parent/childTwo";

    success = target.createNode(nestedPathOne);
    assertTrue("Failed to create parent and child node", success);

    assertTrue("Parent node was not created", target.nodeExists(parentPath));

    success = target.createNode(nestedPathTwo);
    assertTrue("Failed to create second node under parent", success);

    target.deleteNode(nestedPathOne);
    assertFalse("First child node unsuccessfully deleted", target.nodeExists(nestedPathOne));

    target.deleteNode(nestedPathTwo);
    assertFalse("Second child node unsuccessfully deleted", target.nodeExists(nestedPathTwo));

    target.deleteNode(parentPath);
    assertFalse("Parent node unsuccessfully deleted", target.nodeExists(parentPath));
  }

  @Test
  public void testGetChildrenOfParentNode() {
    boolean success = false;
    String parentPath = "/parent";
    String nestedPathOne = "/parent/childOne";
    String nestedPathTwo = "/parent/childTwo";

    success = target.createNode(nestedPathOne);
    assertTrue("Failed to create parent and child node", success);

    assertTrue("Parent node was not created", target.nodeExists(parentPath));

    success = target.createNode(nestedPathTwo);
    assertTrue("Failed to create second node under parent", success);

    List<String> childrenPaths = target.getChildren(parentPath);
    assertTrue("childOne path not successfully retrieved", childrenPaths.contains(nestedPathOne.split("\\/")[2]));
    assertTrue("childTwo path not successfully retrieved", childrenPaths.contains(nestedPathTwo.split("\\/")[2]));
    assertFalse("Random path retrieved when not created", childrenPaths.contains("random"));

    target.deleteNode(nestedPathOne);
    assertFalse("First child node unsuccessfully deleted", target.nodeExists(nestedPathOne));

    target.deleteNode(nestedPathTwo);
    assertFalse("Second child node unsuccessfully deleted", target.nodeExists(nestedPathTwo));

    target.deleteNode(parentPath);
    assertFalse("Parent node unsuccessfully deleted", target.nodeExists(parentPath));
  }

  @Test
  public void testTrackingTastStatusData() {
    boolean success = false;
    String path = "/taskStatus";
    String id = "id";
    Protos.TaskID taskId = Protos.TaskID.newBuilder()
                                        .setValue(id)
                                        .build();
    Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
                                                .setTaskId(taskId)
                                                .setState(Protos.TaskState.TASK_RUNNING)
                                                .build();

    success = target.createNode(path);
    assertTrue("Failed to create node at specified path", success);

    success = target.updateNodeData(path, id);
    assertTrue("Failed to update node data at specified path", success);

    Protos.TaskID taskIdClone = Protos.TaskID.newBuilder()
                                             .setValue(new String(target.getNodeData(path)))
                                             .build();
    Protos.TaskStatus statusClone = Protos.TaskStatus.newBuilder()
                                                .setTaskId(taskIdClone)
                                                .setState(Protos.TaskState.TASK_RUNNING)
                                                .build();

    Protos.TaskID taskIdFake = Protos.TaskID.newBuilder()
            .setValue("fake")
            .build();
    Protos.TaskStatus statusFake = Protos.TaskStatus.newBuilder()
            .setTaskId(taskIdFake)
            .setState(Protos.TaskState.TASK_RUNNING)
            .build();

    assertTrue("TaskStatus objects stored in ZooKeeper do not match existing ones", status.equals(statusClone));
    assertFalse("Fake TaskStatus objects match the ones stored in ZooKeeper", status.equals(statusFake));

    target.deleteNode(path);
    assertFalse("Node unsuccessfully deleted", target.nodeExists(path));
  }

  @After
  public void closeConnection() {
    target.close();
  }
}
