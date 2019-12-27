package org.csource.fastdfs.pool;

import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.ProtoCommon;
import org.csource.fastdfs.TrackerServer;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ConnectionManager {
    /**
     * ip:port is key
     */
    private String key;

    /**
     * total create connection pool
     */
    private AtomicInteger totalCount = new AtomicInteger();

    /**
     * free connection count
     */
    private AtomicInteger freeCount = new AtomicInteger();

    /**
     * lock
     */
    private ReentrantLock lock = new ReentrantLock(true);

    private Condition condition = lock.newCondition();

    /**
     * free connections
     */
    private volatile ConcurrentLinkedQueue<ConnectionInfo> freeConnections = new ConcurrentLinkedQueue<ConnectionInfo>();

    private ConnectionManager() {

    }

    public ConnectionManager(String key) {
        this.key = key;
    }

    private synchronized ConnectionInfo newConnection() throws IOException {
        try {
            ConnectionInfo connectionInfo = PoolConnectionFactory.create(this.key);
            return connectionInfo;

        } catch (IOException e) {
            throw e;
        }
    }


    public synchronized ConnectionInfo getConnection() throws IOException {
        lock.lock();
        try {
            ConnectionInfo connectionInfo = null;
            while (true) {
                if (freeCount.get() > 0) {
                    connectionInfo = freeConnections.poll();
                    if ((System.currentTimeMillis() - connectionInfo.getLastAccessTime()) > ClientGlobal.getG_connection_pool_max_idle_time()) {
                        closeConnection(connectionInfo);
                        continue;
                    } else {
                        freeCount.decrementAndGet();
                    }
                } else if (ClientGlobal.getG_connection_pool_max_count_per_entry() == 0 || totalCount.get() < ClientGlobal.getG_connection_pool_max_count_per_entry()) {
                    connectionInfo = newConnection();
                    if (connectionInfo != null) {
                        totalCount.incrementAndGet();
                    }
                } else {
                    try {
                        if (condition.await(ClientGlobal.getG_connection_pool_max_wait_time(), TimeUnit.MILLISECONDS)) {
                            //wait single success
                            continue;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //if need check active
                if (connectionInfo.isNeedActiveCheck()) {
                    boolean activeYes = ProtoCommon.activeTest(connectionInfo.getSocket());
                    if (activeYes) {
                        connectionInfo.setLastAccessTime(System.currentTimeMillis());
                    } else {
                        //close if check fail
                        closeConnection(connectionInfo);
                        continue;
                    }
                }
                return connectionInfo;
            }
        } catch (IOException e) {
            return null;
        } finally {
            lock.unlock();
        }
    }

    public  void freeConnection(TrackerServer trackerServer) throws IOException {
        if (trackerServer == null || !trackerServer.isConnected()) {
            return;
        }
        ConnectionInfo connectionInfo = new ConnectionInfo(trackerServer.getSocket(),trackerServer.getInetSocketAddress(),System.currentTimeMillis(),true);
        if ((System.currentTimeMillis() - trackerServer.getLastAccessTime()) < ClientGlobal.getG_connection_pool_max_idle_time()) {
            try {
                lock.lock();
                freeConnections.add(connectionInfo);
                freeCount.incrementAndGet();
                condition.signal();
            } finally {
                lock.unlock();
            }
        } else {
            closeConnection(connectionInfo);
        }
    }

    public void closeConnection(ConnectionInfo connectionInfo) throws IOException {
        if (connectionInfo.getSocket() != null) {
            totalCount.decrementAndGet();
            try {
                ProtoCommon.closeSocket(connectionInfo.getSocket());
            }  finally {
                connectionInfo.setSocket(null);
            }
        }
    }

    @Override
    public String toString() {
        return "ConnectionManager{" +
                "key='" + key + '\'' +
                ", totalCount=" + totalCount +
                ", freeCount=" + freeCount +
                ", linkedQueueCP=" + freeConnections +
                '}';
    }
}