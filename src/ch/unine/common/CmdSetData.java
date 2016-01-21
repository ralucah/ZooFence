package ch.unine.common;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class CmdSetData extends Command {

	private static final long serialVersionUID = 1L;
	private byte[] data;
	private int version;
	
	public CmdSetData(String id, String path, byte[] data, int version) {
		super(id, path, CmdType.SET_DATA);
		this.data = data;
		this.version = version;
	}
	
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException {
		Stat stat = zk.setData(path, data, version);
		
		if (stat != null)
			return stat;
		
		return null;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
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
	}
}
