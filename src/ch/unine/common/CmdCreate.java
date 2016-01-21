package ch.unine.common;


import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

public class CmdCreate extends Command {
	
	private static final long serialVersionUID = 1L;

	private enum CreateModeType {PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL, EPHEMERAL_SEQUENTIAL};
	
	private CreateModeType createMode;
	private byte[] data;
	private List<byte[]> aclSerialized;
	
	public CmdCreate(String id, String path, byte[] data, List<ACL> acl, CreateMode cMode) {
		super(id, path, CmdType.CREATE);
		this.data = data;
		this.aclSerialized = Serializer.serializeACL(acl);

		switch(cMode) {
		case PERSISTENT: 
			createMode = CreateModeType.PERSISTENT;
			break;
		case PERSISTENT_SEQUENTIAL:
			createMode = CreateModeType.PERSISTENT_SEQUENTIAL;
			break;
		case EPHEMERAL:
			createMode = CreateModeType.EPHEMERAL;
			break;
		case EPHEMERAL_SEQUENTIAL:
			createMode = CreateModeType.EPHEMERAL_SEQUENTIAL;
			break;
		}
	}
	
	@Override
	public Object execute(ZooKeeper zk) throws KeeperException, InterruptedException, IllegalArgumentException {
		CreateMode cMode = null;
		switch(createMode) {
		case PERSISTENT:
			cMode = CreateMode.PERSISTENT;
			break;
		case PERSISTENT_SEQUENTIAL:
			cMode = CreateMode.PERSISTENT_SEQUENTIAL;
			break;
		case EPHEMERAL:
			cMode = CreateMode.EPHEMERAL;
			break;
		case EPHEMERAL_SEQUENTIAL:
			cMode = CreateMode.EPHEMERAL_SEQUENTIAL;
			break;
		}
		
		List<ACL> acl =  Serializer.deserializeACL(aclSerialized);
		
		String result = zk.create(path, data, acl, cMode);
		
		return result;
	}

	private class ResultHandler implements AsyncCallback.StringCallback {

		@Override
		public void processResult(int rc, String path, Object ctx, String name) {
			List<Object> results = (List<Object>) ctx;
			synchronized (results) {
				results.add(path);
				results.notifyAll();
			}
		}
		
	}
	
	@Override
	public void executeAsync(ZooKeeper zk, List<Object> results) throws KeeperException,
			InterruptedException {		
		CreateMode cMode = null;
		switch(createMode) {
		case PERSISTENT:
			cMode = CreateMode.PERSISTENT;
			break;
		case PERSISTENT_SEQUENTIAL:
			cMode = CreateMode.PERSISTENT_SEQUENTIAL;
			break;
		case EPHEMERAL:
			cMode = CreateMode.EPHEMERAL;
			break;
		case EPHEMERAL_SEQUENTIAL:
			cMode = CreateMode.EPHEMERAL_SEQUENTIAL;
			break;
		}
		
		List<ACL> acl =  Serializer.deserializeACL(aclSerialized);
		
		ResultHandler rh = new ResultHandler();		
		zk.create(path, data, acl, cMode, rh, results);
	}
}
