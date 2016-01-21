package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class CmdExists extends Command {

	private static final long serialVersionUID = 1L;
	private Object watcher;
	private String watcherType;

	public CmdExists(String id, String path, Watcher watcher) {
		super(id, path, CmdType.EXISTS);
		this.watcher = watcher;
		watcherType = "Watcher";
	}
	
	public CmdExists(String id, String path, boolean watcher) {
		super(id, path, CmdType.EXISTS);
		this.watcher = watcher;
		watcherType = "boolean";
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {		
		Stat stat = null;
		if (watcherType.equals("Watcher")) {
			Watcher w  = null;
			if (watcher != null)
				w = (Watcher) watcher;
			stat = zk.exists(path, w);
		} else {
			boolean w = (boolean) watcher;
			stat = zk.exists(path, w);
		}
		
		return stat;
	}

	@Override
	public void executeAsync(ZooKeeper zk, List<Object> results)
			throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
}
