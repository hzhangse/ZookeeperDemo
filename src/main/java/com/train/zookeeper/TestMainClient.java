package com.train.zookeeper;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * TestMainClient
 * <p/>
 */
public class TestMainClient implements Watcher {
	protected static ZooKeeper zk = null;
	protected static Integer mutex;
	int sessionTimeout = 10000;
	protected String root;

	public TestMainClient(String connectString) {
		if (zk == null) {
			try {

				String configFile = this.getClass().getResource("/").getPath()
						+ "com/train/zookeeper/log4j/log4j.xml";
				DOMConfigurator.configure(configFile);
				System.out.println("创建一个新的连接:");
				zk = new ZooKeeper(connectString, sessionTimeout, this);
				mutex = new Integer(-1);
			} catch (IOException e) {
				zk = null;
			}
		}
	}

	synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}
}
