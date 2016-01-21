package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkexecutor.TCPServer;

public class AsyncResultsThread extends Thread {
	protected List<Object> results;
	protected List<ZooKeeper> zks;
	protected LogEntry logEntry;
	protected Log log;
	protected TCPServer server;
	public AsyncResultsThread(List<Object> results, List<ZooKeeper> zks, LogEntry logEntry, Log log, TCPServer server) {
		this.results = results;
		this.zks = zks;
		this.logEntry = logEntry;
		this.log = log;
		this.server = server;
	}
}
