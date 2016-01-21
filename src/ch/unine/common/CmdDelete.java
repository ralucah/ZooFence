package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class CmdDelete extends Command {

	private static final long serialVersionUID = 1L;
	private int version;
	
	public CmdDelete(String id, String path, int version) {
		super(id, path, CmdType.DELETE);
		this.version = version;
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws InterruptedException, KeeperException {	
		zk.delete(path, version);

		return null;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public void executeAsync(ZooKeeper zk, List<Object> results)
			throws KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		
	}
}
