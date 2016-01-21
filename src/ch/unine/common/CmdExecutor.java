package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class CmdExecutor extends Thread {
	private Command cmd;
	private ZooKeeper zk;
	private List<Object> results;
	
	public CmdExecutor(Command cmd, ZooKeeper zk, List<Object> results) {
		this.cmd = cmd;
		this.zk = zk;
		this.results = results;
	}

	@Override
	public void run() {	
		Object result = null;
		
		try {
			result = cmd.execute(zk);
		} catch (KeeperException | InterruptedException | IllegalArgumentException e) {
			result = e;
		} 
		finally {
			
			synchronized (results) {
				results.add(result);
				results.notifyAll();
			}
		}
	}
}
