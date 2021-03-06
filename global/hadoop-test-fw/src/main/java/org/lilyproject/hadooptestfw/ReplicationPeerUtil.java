package org.lilyproject.hadooptestfw;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public class ReplicationPeerUtil implements ReplicationPeerUtilMBean {
    @Override
    public void waitOnReplicationPeerReady(String peerId) {
        long tryUntil = System.currentTimeMillis() + 60000L;
        boolean waited = false;
        while (!threadExists(".replicationSource," + peerId)) {
            waited = true;
            if (System.currentTimeMillis() > tryUntil) {
                throw new RuntimeException("Replication thread for peer " + peerId + " didn't start within timeout.");
            }
            System.out.print("\nWaiting for replication source for " + peerId + " to be started...");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // I don't expect this
                throw new RuntimeException(e);
            }
        }

        if (waited)
            System.out.println("done");
    }

    @Override
    public void waitOnReplicationPeerStopped(String peerId) {
        long tryUntil = System.currentTimeMillis() + 60000L;
        boolean waited = false;
        while (threadExists(".replicationSource," + peerId)) {
            waited = true;
            if (System.currentTimeMillis() > tryUntil) {
                throw new RuntimeException("Replication thread for peer " + peerId + " didn't stop within timeout.");
            }
            System.out.print("\nWaiting for replication source for " + peerId + " to be stopped...");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // I don't expect this
                throw new RuntimeException(e);
            }
        }

        if (waited)
            System.out.println("done");

        try {
            MBeanServerConnection connection = java.lang.management.ManagementFactory.getPlatformMBeanServer();
            ObjectName replicationSourceMBean = new ObjectName("hadoop:service=Replication,name=ReplicationSource for " + peerId);
            connection.unregisterMBean(replicationSourceMBean);
        } catch (Exception e) {
            throw new RuntimeException("Error removing replication source mean for " + peerId, e);
        }
    }

    private static boolean threadExists(String namepart) {
        ThreadMXBean threadmx = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = threadmx.getThreadInfo(threadmx.getAllThreadIds());
        for (ThreadInfo info : infos) {
            if (info != null) { // see javadoc getThreadInfo (thread can have disappeared between the two calls)
                if (info.getThreadName().contains(namepart)) {
                    return true;
                }
            }
        }
        return false;
    }
}
