package ch.unine.common;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;


public class Log {

	private ZooKeeper zookeeper;	// zk on which log is created
	private String key;				// key appended to log
	private String logNode;			// name of log node
	private int logEntryId = 1;
	
	private String executorAddress;
	private static List<String> cachedLogEntries = null;

	/**
	 * Constructor
	 * @param zookeeper instance on which the log + result nodes will be created
	 * @param key appended to /log
	 */
	public Log(ZooKeeper zookeeper, String key) {
		this.zookeeper = zookeeper;
		this.key = key;
		
		
		logNode = "/log" + key;
		
		try {
			if (zookeeper.exists(logNode, false) == null) {
				zookeeper.create(logNode, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				//ZooKeeperPartitioned.leader = true;
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		catch (KeeperException e) {
			// ignore it!
		}
	}
	
	public synchronized void add(LogEntry logEntry) {
		try {
			zookeeper.create(logNode + "/cmd", Serializer.serialize(logEntry), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private class ResultHandler implements AsyncCallback.VoidCallback {

		@Override
		public void processResult(int rc, String path, Object ctx) {
			//nothing to do
		}
		
	}
	
	public synchronized void remove(LogEntry logEntry) {
			//ZKUtil.deleteRecursive(zookeeper, logEntry.getLogPath());
			ResultHandler rh = new ResultHandler();
			zookeeper.delete(logEntry.getLogPath(), -1, rh, null);
	}
	
	public synchronized LogEntry getNext() {		
		LogEntry logEntry = null;
		
		if (cachedLogEntries == null || cachedLogEntries.size() == 0) {
			try {
				cachedLogEntries = zookeeper.getChildren(logNode, false);
				cachedLogEntries.remove("leader");
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if (cachedLogEntries.size() == 0)
			return null;
		else {
			String toReturn = Collections.min(cachedLogEntries);
			cachedLogEntries.remove(toReturn);
			
			try {
				logEntry = (LogEntry)Serializer.deserialize(zookeeper.getData(logNode + "/" + toReturn , false, null));
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logEntry.setPath(logNode + "/" + toReturn);
			return logEntry;
		}
		
		/*
		int cmdIdLength = String.valueOf(logEntryId).length();
		String padding = "";
		int counter = 0;
		while (counter + cmdIdLength < 10) {
			padding += "0";
			counter++;
		}
		
		String nextLogEntry = logNode + "/cmd" + padding + logEntryId;
		
		try {
			if (zookeeper.exists(nextLogEntry, false) == null)
				return null;
			
			logEntry = (LogEntry)Serializer.deserialize(zookeeper.getData(nextLogEntry , false, null));
			logEntry.setPath(nextLogEntry);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		
		logEntryId++;
		
		return logEntry;
		*/
	}
	
	public String getKey() {
		return key;
	}
	
	public String getLogNode() {
		return logNode;
	}
	
	public String getExecutorAddress() {
		if (executorAddress == null) {
			try {
				executorAddress = new String(zookeeper.getData(logNode + "/leader", false, null), "UTF-16");
			} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		
		return executorAddress;
	}
}
