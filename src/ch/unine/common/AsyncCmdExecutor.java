package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

public class AsyncCmdExecutor extends Thread {
	private Command cmd;
	private ZooKeeper zk;
	private List<Object> results;
	
	public AsyncCmdExecutor(Command cmd, ZooKeeper zk, List<Object> results) {
		this.cmd = cmd;
		this.zk = zk;
		this.results = results;
	}

	@Override
	public void run() {	
		try {
			cmd.executeAsync(zk, results);
		} catch (KeeperException | InterruptedException | IllegalArgumentException e) {
			synchronized (results) {
				results.add(e);
				results.notifyAll();				
			}
			
		} 
	}
}
