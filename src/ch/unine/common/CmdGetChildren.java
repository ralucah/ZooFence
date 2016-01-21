package ch.unine.common;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class CmdGetChildren extends Command {

	private static final long serialVersionUID = 1L;
	private Object watcher;
	private String watcherType;
	
	public CmdGetChildren(String id, String path, Watcher watcher) {
		super(id, path, CmdType.GET_CHILDREN);
		this.watcher = watcher;
		watcherType = "Watcher";
	}

	public CmdGetChildren(String id, String path, boolean watcher) {
		super(id, path, CmdType.GET_CHILDREN);
		this.watcher = watcher;
		watcherType = "boolean";
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {
		List<String> children = new ArrayList<String>();
		if (watcherType == "Watcher") {
			Watcher w = (Watcher) watcher;
			List<String> crtChildren = zk.getChildren(path, w);
			
			for (String child : crtChildren) {
				if (!children.contains(child))
					children.add(child);
			}
		} else {
			boolean w = (boolean) watcher;
			List<String> crtChildren = zk.getChildren(path, w);
			
			for (String child : crtChildren) {
				if (!children.contains(child))
					children.add(child);
			}
		}
		
		return children;
	}

	@Override
	public void executeAsync(ZooKeeper zk, List<Object> results)
			throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
}
