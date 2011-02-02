/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.util.zookeeper;

import java.io.IOException;
import java.util.Arrays;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;

/**
 * Various ZooKeeper utility methods.
 */
public class ZkUtil {
    
    public static ZooKeeperItf connect(String connectString, int sessionTimeout) throws ZkConnectException {
        ZooKeeperImpl zooKeeper;
        try {
            zooKeeper = new ZooKeeperImpl(connectString, sessionTimeout);
        } catch (IOException e) {
            throw new ZkConnectException("Failed to connect with Zookeeper @ <" + connectString + ">", e);
        }
        long waitUntil = System.currentTimeMillis() + sessionTimeout;
        boolean connected = (States.CONNECTED).equals(zooKeeper.getState());
        while (!connected && waitUntil > System.currentTimeMillis()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                connected = (States.CONNECTED).equals(zooKeeper.getState());
                break;
            }
            connected = (States.CONNECTED).equals(zooKeeper.getState());
        }
        if (!connected) {
            System.out.println("Failed to connect to Zookeeper within timeout: Dumping stack: ");
            Thread.dumpStack();
            zooKeeper.close();
            throw new ZkConnectException("Failed to connect with Zookeeper @ <" + connectString +
                    "> within timeout <" + sessionTimeout + ">");
        }
        return zooKeeper;
    }

    public static void createPath(final ZooKeeperItf zk, final String path)
            throws InterruptedException, KeeperException {
        createPath(zk, path, null);
    }

    /**
     * Creates a persistent path on zookeeper if it does not exist yet, including any parents.
     * Keeps retrying in case of connection loss.
     *
     * <p>The supplied data is used for the last node in the path. If the path already exists,
     * the data is updated if necessary.
     */
    public static void createPath(final ZooKeeperItf zk, final String path, final byte[] data)
            throws InterruptedException, KeeperException {

        if (!path.startsWith("/"))
            throw new IllegalArgumentException("Path should start with a slash.");

        if (path.endsWith("/"))
            throw new IllegalArgumentException("Path should not end on a slash.");

        String[] parts = path.substring(1).split("/");

        final StringBuilder subPath = new StringBuilder();
        boolean created = false;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            subPath.append("/").append(part);

            // Only use the supplied data for the last node in the path
            final byte[] newData = (i == parts.length - 1 ? data : null);

            created = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                public Boolean execute() throws KeeperException, InterruptedException {
                    if (zk.exists(subPath.toString(), false) == null) {
                        try {
                            zk.create(subPath.toString(), newData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            return true;
                        } catch (KeeperException.NodeExistsException e) {
                            return false;
                        }
                    }
                    return false;
                }
            });
        }

        if (!created) {
            // The node already existed, update its data if necessary
            zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                public Boolean execute() throws KeeperException, InterruptedException {
                    byte[] currentData = zk.getData(path, false, new Stat());
                    if (!Arrays.equals(currentData, data)) {
                        zk.setData(path, data, -1);
                    }
                    return null;
                }
            });
        }
    }
}
