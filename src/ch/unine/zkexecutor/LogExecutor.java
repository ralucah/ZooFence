package ch.unine.zkexecutor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.NotEmptyException;
import org.apache.zookeeper.data.Stat;

import ch.unine.common.AsyncCmdExecutor;
import ch.unine.common.AsyncResultsThread;
import ch.unine.common.CmdExecutor;
import ch.unine.common.CmdExists;
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
import ch.unine.common.Command.CmdType;

public class LogExecutor {
	
	private Log log;
	private MappingFunction mappingFunc;
	
	private ZooKeeper zkAdmin;
	private TCPServer server;
	
	public LogExecutor(ZooKeeper zkAdmin, Log log, List<ZooKeeper> zookeepers, MappingFunction mappingFunc, int flatteningFactor) {
		this.log = log;
		this.mappingFunc = mappingFunc;

		
		try {
			zkAdmin.create(log.getLogNode().concat("/leader"), Configuration.executorAddress.getBytes("UTF-16"), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		/* start tcp server; one per executor; listens to multiple clients */
		server = new TCPServer(Configuration.executorAddress);
		server.start();
		
		this.zkAdmin = zkAdmin;
	}
	
	public void run() {
		
		/* triggered when */
		Watcher watcher = new Watcher(){
			@Override
			public void process(WatchedEvent event) {
				try {
					zkAdmin.getChildren(log.getLogNode(), this);
				} catch (KeeperException | InterruptedException e1) {
					e1.printStackTrace();
				}
				while (true) {
					LogEntry logEntry = log.getNext();
					if (logEntry == null)
						break;
					
					Object result = null;
					boolean async = false;
					
					if (logEntry != null) {
						Command cmd = logEntry.getCmd();
						CmdType type = cmd.getType();
						String keySync = logEntry.getLogSync();
						
						if (keySync != null) {
							Log logSync = LogManager.getInstance(zkAdmin).getLog(keySync);
							
							logEntry.setLogSync(null);
							
							TCPClient clientConn = new TCPClient(log.getKey(), LogManager.getInstance(zkAdmin).getLog(logSync.getKey()).getExecutorAddress());
							clientConn.startClient();
							
							String clientId = logEntry.getClientId();
							logEntry.setClientId(log.getKey());
							logSync.add(logEntry);
							try {
								result = ResultStore.getInstance().waitForResult(log.getKey(), logEntry.getCmd().getId());
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							logEntry.setClientId(clientId);
							
							clientConn.close();
						} else {
							// map to zookeepers and to log
							List<ZooKeeper> zks = mappingFunc.getZks(logEntry.getLogKey());
							
							// check if node doesn't have children on any partition
							if (type.equals(CmdType.DELETE)) {
								Command existsCmd = new CmdExists(cmd.getId(), cmd.getPath(), true);
								List<Object> existsResults = new ArrayList<Object>();
								
								synchronized (existsResults){
									for (ZooKeeper zk : zks) {				
										CmdExecutor cmdExecutor = new CmdExecutor(existsCmd, zk, existsResults);
										cmdExecutor.start();
									}
									
									while (existsResults.size() < zks.size()) {
										try {
											existsResults.wait();
										} catch (InterruptedException e) {
											e.printStackTrace();
										}
									}
								}
								
								for (Object res : existsResults) {
									Stat stat = (Stat) res;
									if (stat.getNumChildren() != 0) {
										result = new NotEmptyException(cmd.getPath());
										break;
									}
								}
							}
							
							
							if (result == null) {
								List<Object> results = new ArrayList<Object>();
								
								if (type.equals(CmdType.CREATE)) {
									async = true;
									// execute command
									synchronized (results) {
										for (ZooKeeper zk : zks) {				
											//AsyncCmdExecutor cmdExecutor = new AsyncCmdExecutor(cmd, zk, results);
											//cmdExecutor.start();
											try {
												cmd.executeAsync(zk, results);
											} catch (KeeperException
													| InterruptedException e) {
												e.printStackTrace();
											}
										}
									}
									
									AsyncResultsThread resultsThread=new AsyncResultsThread(results, zks, logEntry, log, server){
							            public void run() {
							            	synchronized(results) {
							            		while (results.size() < zks.size()) {							            		
													try {
															results.wait();
														} catch (InterruptedException e) {
															e.printStackTrace();
														}
							            		}
											}
							            	server.replyToClient(logEntry.getClientId(), logEntry.getCmd().getId(), Serializer.serialize(results.get(0)));
							            }
									};
									resultsThread.start();
									log.remove(logEntry);
									
								} else {
									// execute command
									synchronized (results) {
										for (ZooKeeper zk : zks) {				
											CmdExecutor cmdExecutor = new CmdExecutor(cmd, zk, results);
											cmdExecutor.start();
										}
										
										// wait for all zookeepers to complete the execution of the command
										while (results.size() < zks.size()) {
										try {
												results.wait();
											} catch (InterruptedException e) {
												e.printStackTrace();
											}
										}
									}
								}
							
								if (async == false && results.get(0) != null && !(results.get(0)).toString().contains("Exception")) {
									switch(type) {
									case EXISTS:
										Stat stat = new Stat();
										for (Object res : results) {
											if (res != null) {
												Stat statTmp = (Stat) res;
												
												// get latest modified stat
												if (stat.getMtime() < statTmp.getMtime()) {
													stat.setAversion(statTmp.getAversion());
													stat.setCtime(statTmp.getCtime());
													stat.setCversion(statTmp.getCversion());
													stat.setCzxid(statTmp.getCzxid());
													stat.setDataLength(statTmp.getDataLength());
													stat.setEphemeralOwner(statTmp.getEphemeralOwner());
													stat.setMtime(statTmp.getMtime());
													stat.setMzxid(statTmp.getMzxid());
													stat.setPzxid(statTmp.getPzxid());
													stat.setVersion(statTmp.getVersion());
												}
												// add num children
												stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
											}
										}
											
										result = Serializer.serializeStat(stat);
										break;
									case GET_CHILDREN:
										List<String> children = new ArrayList<String>();
										
										for (Object res : results) {
											List<String> childenTmp = (List<String>)res;
											
											for (String s : childenTmp) {
												if (children.contains(s) == false) {
													children.add(s);
												}
											}
										}
										
										result = children;
										break;
									case GET_DATA:
										stat = new Stat();
										byte[] data = null;
										
										for (Object res : results) {
											if (data == null)
												data = ((Data)res).getData();
											
											Stat statTmp = ((Data)res).getStat();
											
											if (stat.getMtime() < statTmp.getMtime()) {
												stat.setAversion(statTmp.getAversion());
												stat.setCtime(statTmp.getCtime());
												stat.setCversion(statTmp.getCversion());
												stat.setCzxid(statTmp.getCzxid());
												stat.setDataLength(statTmp.getDataLength());
												stat.setEphemeralOwner(statTmp.getEphemeralOwner());
												stat.setMtime(statTmp.getMtime());
												stat.setMzxid(statTmp.getMzxid());
												stat.setPzxid(statTmp.getPzxid());
												stat.setVersion(statTmp.getVersion());
											}
											
											// add children number
											stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
										}
										
										result = Serializer.serializeData(new Data(data, stat));
										break;
									case SET_DATA:
										stat = new Stat();
										for (Object res : results) {
											Stat statTmp = (Stat) res;
											
											if (stat.getMtime() < statTmp.getMtime()) {
												stat.setAversion(statTmp.getAversion());
												stat.setCtime(statTmp.getCtime());
												stat.setCversion(statTmp.getCversion());
												stat.setCzxid(statTmp.getCzxid());
												stat.setDataLength(statTmp.getDataLength());
												stat.setEphemeralOwner(statTmp.getEphemeralOwner());
												stat.setMtime(statTmp.getMtime());
												stat.setMzxid(statTmp.getMzxid());
												stat.setPzxid(statTmp.getPzxid());
												stat.setVersion(statTmp.getVersion());
											}
											
											// add children number
											stat.setNumChildren(stat.getNumChildren() + statTmp.getNumChildren());
										}
										
										result = Serializer.serializeStat(stat);
										break;
										
									default:
										result = results.get(0);
										break;
									}
								} else if (async == false) {
									result = results.get(0);
								}
							}
						}
						
						if (async == false) {
							server.replyToClient(logEntry.getClientId(), logEntry.getCmd().getId(), Serializer.serialize(result));
							log.remove(logEntry);
						}
					}
					
					
				}
				
			}
		};

		// attach watcher
		try {
			zkAdmin.getChildren(log.getLogNode(), watcher);
		} catch (KeeperException | InterruptedException e1) {
		}
	
		// don't quit; sleep for 1h
		try {
			Thread.sleep(3600000);
		} catch (InterruptedException e) {
			System.err.println("Executor of log " + this.log.getKey() + " timeout!");
		}
	}
	
	// args: log key
	public static void main(String[] args) {
		/* config */
		Configuration.setup();
		
		Watcher watcher = new Watcher(){
			public void process(WatchedEvent event) {
			}
		};
		
		/* initialize array of zk instances (the "partitions") */
		List<ZooKeeper> zookeepers = new ArrayList<ZooKeeper>();
		for (String conn : Configuration.zksConnectStrings) {
			ZooKeeper zk = null;
			try {
				zk = new ZooKeeper(conn, 1000, watcher);
			} catch (IOException e) {
				e.printStackTrace();
			}
			zookeepers.add(zk);
		}
		
		ZooKeeper zkAdmin = null;
		try {
			zkAdmin = new ZooKeeper(Configuration.zkAdminConnectString, 1000, watcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	    Log log = new Log(zkAdmin, Configuration.executorLog);
		
		/* initialize mapping function */
		MappingFunction mappingFunc = new MappingFunction(zookeepers, Configuration.flatteningFactor, Configuration.reductionFactor);
		
		LogExecutor logExecutor = new LogExecutor(zkAdmin, log, zookeepers, mappingFunc, Configuration.flatteningFactor);
		logExecutor.run();
	}
}