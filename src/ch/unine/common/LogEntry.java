package ch.unine.common;

import java.io.Serializable;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class LogEntry implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Command cmd;		// the command
	private String logPath;		// the log path
	private String logKey;		// the log key
	private String logSync;		// the key of the log to keep in sync with
	private String clientId;

	public LogEntry(Command cmd, String clientId) {
		this.cmd = cmd;
		this.clientId = clientId;
	}

	public Command getCmd() {	 
		return cmd;
	}

	public String getLogPath() {
		return logPath;
	}

	public void setPath(String logPath) {
		this.logPath = logPath;
	}

	public String getLogSync() {
		return logSync;
	}

	public void setLogSync(String logSync) {
		this.logSync = logSync;
	}

	public String getLogKey() {
		return logKey;
	}

	public void setLogKey(String logKey) {
		this.logKey = logKey;
	}
	
	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
}
