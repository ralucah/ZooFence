package ch.unine.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.ZooKeeper;


public class LogManager {
	
	private static LogManager instance = null;
	private Map<String, Log> logs;
	private ZooKeeper zkAdmin;

	private LogManager(ZooKeeper zkAdmin) {
		logs = new HashMap<String, Log>();
		this.zkAdmin = zkAdmin;
	}
	
	public static synchronized LogManager getInstance(ZooKeeper zkAdmin) {
		if (instance == null) {
			synchronized (LogManager .class){
				if (instance == null) {
					instance = new LogManager(zkAdmin);
				}
			}
		}
		return instance;
	}

	public synchronized Log getLog(String key) {
		if (key.length() == 1)
			return null;
		
		Log log = logs.get(key);
		
		if (log == null) {
			log = new Log(zkAdmin, key);
			logs.put(key, log);
			// TODO add self as leader of this log
			// TODO if log leader, start log executor
			/*if (ZooKeeperPartitioned.leader) {
				LogExecutor logExecutor = new LogExecutor(log, ZooKeeperPartitioned.zookeepers, ZooKeeperPartitioned.mappingFunc, Configuration.flatteningFactor);
				logExecutor.start();
			}*/
		}
		
		return log;
	}
}
