package ch.unine.common;

import java.util.HashMap;

public class ResultStore {
	private HashMap<String, Object> resultsReceived;
	private static ResultStore instance = null;
	
	private ResultStore() {
		resultsReceived = new HashMap<String, Object>();
	}
	
	public static ResultStore getInstance() {
		if (instance == null)
			instance = new ResultStore();
		return instance;
	}
	
	public void addResult(String clientID, String cmdID, Object result) {
		synchronized (resultsReceived) {
			resultsReceived.put(clientID + cmdID, result);
			resultsReceived.notifyAll();
		}		
	}
	
	public Object waitForResult(String clientID, String cmdID) throws InterruptedException {
		Object result = null;
		synchronized (resultsReceived) {
			if (resultsReceived.containsKey(clientID + cmdID)) {
				result = resultsReceived.remove(clientID + cmdID);
				return result;
			}
			
			boolean found = false;
			while (!found) {
				resultsReceived.wait();
				if (resultsReceived.containsKey(clientID + cmdID)) {
					result = resultsReceived.remove(clientID + cmdID);
					found = true;
				}
			}
		}
		return result;
	}
}
