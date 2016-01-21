package ch.unine.zkpartitioned;

import java.io.IOException;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.lang.*;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import ch.unine.common.CmdCreate;
import ch.unine.common.CmdDelete;
import ch.unine.common.CmdExists;
import ch.unine.common.CmdGetChildren;
import ch.unine.common.CmdGetData;
import ch.unine.common.CmdSetData;
import ch.unine.common.Command;
import ch.unine.common.Configuration;
import ch.unine.common.Data;
import ch.unine.common.Log;
import ch.unine.common.LogEntry;
import ch.unine.common.LogManager;
import ch.unine.common.MappingFunction;
import ch.unine.common.ResultStore;
import ch.unine.common.Serializer;
import ch.unine.common.TCPClient;


public class ZooKeeperPartitioned {
	/* unique client id */
	private String clientId;
	
	/* increasing number that uniquely identifies a command; initialized to 0 in constructor */
	private volatile int cmdNumber;
	
	/* list of zk instances, i.e., partitions */
	private List<ZooKeeper> zookeepers;
	
	private ZooKeeper zkAdmin;
	
	/* function that maps commands to a set of zk instances (partitions) */
	private MappingFunction mappingFunc;
	
	/* in charge with returning the right log */
	private LogManager logManager;
	
	/* one tcp client per log */
	private HashMap<String, TCPClient> tcpClientConns;
	
	private ResultStore resultStore;
	
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	/**
	 * Constructor
	 * @param connectStrings - list of host:ip connection strings
	 * @param sessionTimeout - same as for ZooKeeper
	 * @param watcher - same as for ZooKeeper
	 * @throws IOException
	 */
	public ZooKeeperPartitioned (List<String> connectStrings, int sessionTimeout, Watcher watcher) throws IOException {			
		/* assign unique id to this process */
		clientId = assignClientId();
		
		/* initialize unique cmd number */
		cmdNumber = 0;
		
		/* config */
		Configuration.setup();
		
		/* initialize array of zk instances (the "partitions") */
		zookeepers = new ArrayList<ZooKeeper>();
		for (String conn : connectStrings) {
			ZooKeeper zk = new ZooKeeper(conn, sessionTimeout, watcher);
			zookeepers.add(zk);
		}
		
		zkAdmin = new ZooKeeper(Configuration.zkAdminConnectString, sessionTimeout, watcher);
		
		/* initialize mapping function */
		mappingFunc = new MappingFunction(zookeepers, Configuration.flatteningFactor, Configuration.reductionFactor);
		
		/* initialize log manager */
		logManager = LogManager.getInstance(zkAdmin);
		
		/* initialize tcp servers hash map */
		tcpClientConns = new HashMap<String, TCPClient>();
		
		resultStore = ResultStore.getInstance();
	}
	
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) 
			throws IllegalArgumentException, KeeperException, InterruptedException {
		
		Command cmd = new CmdCreate(assignCmdId(), path, data, acl, createMode);
		Object result = null;
		
		/* map to zks and log keys */
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		/* trivial cmd */
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		} 
		/* non-trivial cmd */ 
		else {
			/* TODO 
			 * not sure about get(0) and get(1); if the size is fixed, use array! 
			 * but array cannot be passed by reference, so it has to be returned
			 */
			Log log = logManager.getLog(logKeys.get(0));
			
			/* handle case where child is not replicated (no log), but parent is */
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			/* TODO: replace length with something else after changing tokenization */
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			
			// open tcp connection to wait for result
			//String key = logEntry.getLogSync();
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
			//result = log.waitForResult(logEntry);
			//log.removeResult(logEntry);
		}
		
		String actualPath = null;
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			actualPath = (String)result;
		}
			
		return actualPath;
	}
	
	public void delete(String path, int version) throws InterruptedException, KeeperException {
		
		Command cmd = new CmdDelete(assignCmdId(), path, version);
		Object result = null;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		// trivial cmd
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
	}
	
	public Stat exists(String path, Watcher watcher) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdExists(assignCmdId(), path, watcher);
		Object result = null;
				
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		// trivial cmd
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		Stat stat = null;
		if (result != null) {
			if (result.toString().contains("Exception")) {
				ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
				exception.throwException();
			} else {
				stat = Serializer.deserializeStat((byte[])result);
			}
		}
		
		return stat;
	}
	
	public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException {
		Command cmd = new CmdExists(assignCmdId(), path, watcher);
		Object result = null;
		boolean trivial = false;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		Stat stat = null;
		if (result != null) {
			if (result.toString().contains("Exception")) {
				ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
				exception.throwException();
			} else {
				if (trivial)
					stat = (Stat)result;
				else
					stat = Serializer.deserializeStat((byte[])result);
			}
		}

		return stat;
	}

	List<String> getChildren(String path, Watcher watcher) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdGetChildren(assignCmdId(), path, watcher);
		Object result = null;

		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		List<String> children = new ArrayList<String>();
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			children = ((ArrayList<String>)result);
		}
		
		return children;
	}
	
	public List<String> getChildren(String path, boolean watcher) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdGetChildren(assignCmdId(), path, watcher);
		Object result = null;

		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		

			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		List<String> children = new ArrayList<String>();
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		else {
			children = ((ArrayList<String>)result); // error:result is one string
		}

		return children;
	}
	
	public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
		
		CmdGetData cmd = new CmdGetData(assignCmdId(), path, watcher, stat);
		Object result = null;

		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			System.out.println("Trivial cmd, mapped to zk " + logKeys.get(0));
			result = cmd.execute(zks.get(0));
		}
		// non-trivial
		else {
			System.out.println("NON-Trivial cmd");
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
		
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		if (result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		Data data = Serializer.deserializeData((byte[])result);
		
		if (stat != null) {
			Stat statResult = data.getStat();
			
			stat.setAversion(statResult.getAversion());
			stat.setCtime(statResult.getCtime());
			stat.setCversion(statResult.getCversion());
			stat.setCzxid(statResult.getCzxid());
			stat.setDataLength(statResult.getDataLength());
			stat.setEphemeralOwner(statResult.getEphemeralOwner());
			stat.setMtime(statResult.getMtime());
			stat.setMzxid(statResult.getMzxid());
			stat.setNumChildren(statResult.getNumChildren());
			stat.setPzxid(statResult.getPzxid());
			stat.setVersion(statResult.getVersion());
		}

		return data.getData();
	}
	
	public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException {
		
		CmdGetData cmd = new CmdGetData(assignCmdId(), path, watcher, stat);
		Object result = null;
		boolean trivial = false;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			System.out.println("Trivial cmd, mapped to " + logKeys.get(0));
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			System.out.println("Non-Trivial");
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		
			
			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		if (result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		if (trivial)
			return ((Data)result).getData();
		else {
			Data data = Serializer.deserializeData((byte[])result);
			
			if (stat != null) {
				Stat statResult = data.getStat();
				
				stat.setAversion(statResult.getAversion());
				stat.setCtime(statResult.getCtime());
				stat.setCversion(statResult.getCversion());
				stat.setCzxid(statResult.getCzxid());
				stat.setDataLength(statResult.getDataLength());
				stat.setEphemeralOwner(statResult.getEphemeralOwner());
				stat.setMtime(statResult.getMtime());
				stat.setMzxid(statResult.getMzxid());
				stat.setNumChildren(statResult.getNumChildren());
				stat.setPzxid(statResult.getPzxid());
				stat.setVersion(statResult.getVersion());
			}
			
			return data.getData();
		}
	}
	
	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
		
		Command cmd = new CmdSetData(assignCmdId(), path, data, version);
		Object result = null;
		boolean trivial = false;
		
		// map to zookeepers and to log
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		List<String> logKeys = new ArrayList<String>();
		mappingFunc.map(cmd, zks, logKeys);
		
		if (logKeys.size() == 1 && logKeys.get(0).length() == 1 && zks.size() == 1) { // TODO is 2nd check really needed?
			result = cmd.execute(zks.get(0));
			trivial = true;
		}
		// non-trivial
		else {
			// TODO not sure about get(0) and get(1); 
			// if the size is fixed, use array! but array cannot be passed by reference, so it has to be returned
			Log log = logManager.getLog(logKeys.get(0));
			
			//handle case where child is not replicated (no log), but parent is
			if (log == null) {
				log = logManager.getLog(logKeys.get(1));
			}

			LogEntry logEntry = new LogEntry(cmd, clientId);
			logEntry.setLogKey(logKeys.get(0));
			
			//TODO: replace length with something else after change tokenization
			if (logKeys.size() == 2 && logKeys.get(0).length() != 1) {
				String parentKey = logKeys.get(1);				
				logEntry.setLogSync(parentKey);
			}		

			// open tcp connection to wait for result
			String key = log.getKey();
			if (tcpClientConns.get(key) == null) {
				TCPClient clientConn = new TCPClient(clientId, logManager.getLog(key).getExecutorAddress());
				tcpClientConns.put(key, clientConn);
				clientConn.startClient();
			}
			
			log.add(logEntry);
			
			result = resultStore.waitForResult(logEntry.getClientId(), logEntry.getCmd().getId());
		}
		
		if (result != null && result.toString().contains("Exception")) {
			ExceptionWrapper exception = new ExceptionWrapper((Exception)result);
			exception.throwException();
		}
		
		Stat stat = null;
		if (trivial)
			stat = (Stat) result;
		else
			stat = Serializer.deserializeStat((byte[])result);
		
		return stat;
	}
	
	public synchronized String assignCmdId() {
		String cmdId = String.valueOf(cmdNumber);
		cmdNumber++;

		return cmdId;
	}
	
	private String assignClientId() {
		//String pid = String.valueOf(ManagementFactory.getRuntimeMXBean().getName().hashCode());
		String pid = String.valueOf(String.valueOf(Math.random()).hashCode());
		
		return pid;
	}
}
