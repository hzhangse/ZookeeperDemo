package com.train.zookeeper.leaderElec;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import com.train.zookeeper.TestMainClient;
import com.train.zookeeper.TestMainServer;

public class LeaderElection extends TestMainClient {
	public static final Logger logger = Logger.getLogger(LeaderElection.class);

	public LeaderElection(String connectString, String root) {
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

	void findLeader() throws InterruptedException, UnknownHostException,
			KeeperException {
		byte[] leader = null;
		try {

			leader = zk.getData(root + "/leader", true, null);
			
		} catch (KeeperException e) {
			if (e instanceof KeeperException.NoNodeException) {
				logger.error(e);
			} else {
				throw e;
			}
		}
		if (leader != null) {
			following();
		} else {
			String newLeader = null;
			byte[] localhost = InetAddress.getLocalHost().getAddress();
			try {

				newLeader = zk.create(root + "/leader", localhost,
						ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				leader = zk.getData(root + "/leader", true, null);
			} catch (KeeperException e) {
				if (e instanceof KeeperException.NodeExistsException) {
					logger.error(e);
				} else {
					throw e;
				}
			}
			if (newLeader != null) {
				leading();
			} else {
				mutex.wait();
			}
		}
	}

	@Override
	public void process(WatchedEvent event) {
		System.out.println(event.getPath() + ":" + event.getType());
		try {
			zk.getData(root + "/leader", true, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if ((root + "/leader").equals(event.getPath())) {

			if (event.getType() == Event.EventType.NodeCreated) {
				System.out.println("得到通知" + event.getState());
				super.process(event);
				following();
			}
		}
	}

	void leading() {
		System.out.println("成为领导者");
	}

	void following() {
//		try {
//			zk.setData(root + "/leader", "".getBytes(), -1);
//		} catch (KeeperException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		// System.out.println("leader exist , following");
	}

	public static void main(String[] args) {
		// TestMainServer.start();

		String connectString = "192.168.1.105:" + TestMainServer.CLIENT_PORT;

		LeaderElection le = new LeaderElection(connectString, "/GroupMembers");
		while (true) {
			try {
				le.findLeader();
				Thread.sleep(1000);
			} catch (Exception e) {
				logger.error(e);
			}
		}
	}
}
