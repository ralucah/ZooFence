package ch.unine.common;

import java.io.Serializable;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public abstract class Command implements Serializable {

	protected static final long serialVersionUID = 1L;
	
	public enum CmdType {CREATE, DELETE, EXISTS, GET_CHILDREN, GET_DATA, SET_DATA};
	
	protected String id;
	protected String path;
	protected CmdType type;

	public Command(String id, String path, CmdType type) {
		this.id = id;
		this.path = path;
		this.type = type;
	}
	
	public abstract Object execute(ZooKeeper zk) throws KeeperException, InterruptedException;
	
	public abstract void executeAsync(ZooKeeper zk, List<Object> results) throws KeeperException, InterruptedException;

	public String getPath() {		
		return path;
	}

	public CmdType getType() {
		return type;
	}
	
	public String getId() {
		return id;
	}
	
	public String getParentPath() {
		String parentPath = "";
		
		String[] pathTokens = path.split("/");
		for (int i = 1 ; i < pathTokens.length-1; i++) {
			parentPath += "/".concat(pathTokens[i]);
		}
		
		if (parentPath.equals(""))
			parentPath = "/";

		return parentPath;
	}
	
	public int getPathLength() {
		String[] pathTokens = path.split("/");
		return (pathTokens.length - 1);
	}
}
