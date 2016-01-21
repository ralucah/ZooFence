package ch.unine.zkpartitioned.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class SimpleTest {

	public static void main(String[] args) throws IOException {
		
		List<String> connectStrings = new ArrayList<String>();
		connectStrings.add("127.0.0.1:12181");
		connectStrings.add("127.0.0.1:12182");
		
		Watcher watcher = new Watcher(){
			@Override
			public void process(WatchedEvent event) {
			}
			
		};

		ZooKeeperPartitioned zkp = new ZooKeeperPartitioned(connectStrings, 1000, watcher);
		
		try {
			zkp.create("/hello", "hello".getBytes(),  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (IllegalArgumentException | KeeperException
				| InterruptedException e) {
			e.printStackTrace();
		}

		final ZooKeeper zk = new ZooKeeper(connectStrings.get(0), 1000, watcher);
		Watcher watcher2 = new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				System.out.println("watched " + event.getType() + " " + event.getType().name());
				try {
					zk.getChildren("/test", this);
				} catch (KeeperException | InterruptedException e) {
					e.printStackTrace();
				}
				
			}
			
		};

		for (int i = 1; i < 100; i++) {
			
			try {
				String result = zkp.create("/perseq", "hello".getBytes(),  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
				System.out.println("Received " + result);
			} catch (IllegalArgumentException | KeeperException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				String result = zkp.create("/test" + i, "hello".getBytes(),  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("Received " + result);
			} catch (IllegalArgumentException | KeeperException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				String result = zkp.create("/test" + i + "/node"+ i, "hello".getBytes(),  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("Received " + result);
			} catch (IllegalArgumentException | KeeperException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			try {
				String result = zkp.create("/test" + i + "/node" + i + "/nodenode" + i, "hello".getBytes(),  Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("Received " + result);
			} catch (IllegalArgumentException | KeeperException
					| InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			zkp.create("/queue0", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/queue1", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/queue2", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zkp.create("/queue3", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			for (int i = 0; i < 5; i++) {
				zkp.create("/queue0/node", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			}
		} catch (IllegalArgumentException | KeeperException| InterruptedException e) {
			e.printStackTrace();
		}
	}
}
