/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.mesos.logviewer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public final class SocketUrlDetectionTest {

    /**
     * Test Target.
     */
    private SocketUrlDetection target;

    private static ServerSocket serverSocket;

    private static final Integer HOPEFULLY_UNUSED_PORT = 29999;

    /**
     * Implements server socket.
     */
    @BeforeClass
    public static void beforeClass() {
        try {
            serverSocket = new ServerSocket(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * If connection success, return true.
     */
    @Test
    public void isReachableTrue() {
        target = new SocketUrlDetection(serverSocket.getLocalPort());
        boolean actual = target.isReachable();
        assertTrue(actual);
    }

    /**
     * If connection refused, return false.
     * This test is assuming that nothing is listening to the chosen port
     * on the machine where the test is run.
     * If your machine don't listen 29999 port, the test will be successful.
     */
    @Test
    public void isReachableFalse() {
        target = new SocketUrlDetection(HOPEFULLY_UNUSED_PORT);
        boolean actual = target.isReachable();
        assertFalse(actual);
    }

    @Test
    public void getPort() {
        int expectedPort = serverSocket.getLocalPort();
        target = new SocketUrlDetection(expectedPort);
        int actualPort = target.getPort();
        assertThat(actualPort, is(expectedPort));
    }

    /**
     * Close server socket connection.
     */
    @AfterClass
    public static void afterClass() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
