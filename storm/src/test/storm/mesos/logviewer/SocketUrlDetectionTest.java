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
import static org.junit.Assert.*;

public final class SocketUrlDetectionTest {

    /**
     * Test Target.
     */
    private SocketUrlDetection target;

    /**
     * ServerSocket.
     */
    private static ServerSocket serverSocket;

    @BeforeClass
    public static void beforeClass() {
        try {
            serverSocket = new ServerSocket(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isReachable() {
        target = new SocketUrlDetection(serverSocket.getLocalPort());
        boolean actual = target.isReachable();
        assertTrue(actual);
    }

    @Test
    public void isReachableFalse() {
        target = new SocketUrlDetection(9999);
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

    @AfterClass
    public static void afeterClass() {
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}