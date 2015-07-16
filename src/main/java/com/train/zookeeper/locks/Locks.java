package com.train.zookeeper.locks;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import com.train.zookeeper.TestMainClient;
import com.train.zookeeper.TestMainServer;

public class Locks extends TestMainClient {
	public static final Logger logger = Logger.getLogger(Locks.class);
	String myZnode;
	public static int lockNumber = 4;

	public Locks(String connectString, String root) {
		super(connectString);
		this.root = root;
		if (zk != null) {
			try {
				Stat s = zk.exists(root, false);
				if (s == null) {
					zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
							CreateMode.PERSISTENT);
				}
			} catch (KeeperException e) {
				logger.error(e);
			} catch (InterruptedException e) {
				logger.error(e);
			}
		}
	}

	void getLock() throws KeeperException, InterruptedException {
		List<String> list = zk.getChildren(root, false);


		String[] nodes = list.toArray(new String[list.size()]);
		Arrays.sort(nodes);
		String removeNode = root + "/" + nodes[0];
		zk.exists(removeNode, true);
		if (myZnode.equals(removeNode)) {
			doAction(myZnode);
		} else {
			waitForLock(myZnode,removeNode);
		}
	}

	void check() throws InterruptedException, KeeperException {
		myZnode = zk.create(root + "/lock_", new byte[0],
				ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		List<String> list = zk.getChildren(root, false);
		while (list.size() < lockNumber) {
			list = zk.getChildren(root, false);
		}
		getLock();
	}

	void waitForLock(String lower,String  removeNode) throws InterruptedException, KeeperException {
		Stat stat = zk.exists(removeNode, true);
		if (stat != null) {
			synchronized (mutex) {
				mutex.wait();
			}
			
		}
//		else {
//			getLock();
//		}
		getLock();
	}

	@Override
	public void process(WatchedEvent event) {

		if (event.getType() == Event.EventType.NodeDeleted) {
			System.out.println("得到通知: node" + event.getPath() + " deleted" );
			
			super.process(event);
			//doAction(event.getPath());
		}
	}

	/**
	 * 执行其他任务
	 */
	private void doAction(String path) {
	
		System.out.println("同步队列已经得到同步，可以开始执行后面的任务了");
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			
			zk.delete(path, -1);
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TestMainServer.start();
		String connectString = "localhost:" + TestMainServer.CLIENT_PORT;

		Locks lk = new Locks(connectString, "/locks");
		try {
			lk.check();
		} catch (InterruptedException e) {
			logger.error(e);
		} catch (KeeperException e) {
			logger.error(e);
		}
	}

}
