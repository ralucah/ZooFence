package ch.unine.common;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.Semaphore;

public class TCPClient extends Thread {
	private String clientId;
	private Socket clientSocket;
	
	private DataInputStream inFromServer;
	private DataOutputStream outToServer;
	
	private Semaphore synchronizer;
	
	private boolean stopped;
	
	public TCPClient(String clientId, String executorAddress) {
		this.stopped = false;
		this.clientId = clientId;
		
		String[] address = executorAddress.split(":");
		String host = address[0];
		int port = new Integer(address[1]);
		
		try {
			clientSocket = new Socket(host, port);
			
			inFromServer = new DataInputStream(clientSocket.getInputStream());
			outToServer = new DataOutputStream(clientSocket.getOutputStream());
		} catch (IOException e) {
			e.printStackTrace();
		} 
		
		synchronizer = new Semaphore(1);
		setDaemon(true);
	}
	
	public void startClient() {
		try {
			synchronizer.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		this.start();
		
		try {
			synchronizer.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run() {   
		// send client id
		try {
			outToServer.writeBytes(clientId);
			outToServer.writeChar('\n');
			outToServer.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// read back ack
		try {
			String ack = inFromServer.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}   
		
		synchronizer.release();
		// wait for result
		while (!stopped) {
			try {
				String cmdID = inFromServer.readLine();
				cmdID = cmdID.trim();
				int length = inFromServer.readInt();
				byte[] result = new byte[length];
			    if (length > 0) {
			    	inFromServer.readFully(result);
			    }
			    Object objResult = Serializer.deserialize(result);
			    ResultStore.getInstance().addResult(clientId, cmdID, objResult);
			} catch (IOException e) {
				System.out.println("Got IOException" + e.getMessage());
				//e.printStackTrace();
			}
		}
		
		//clientSocket.close();
	}
	
	public void close() {
		stopped = true;
		try {
			outToServer.flush();
			outToServer.close();
			inFromServer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	protected void finalize() {
		close();
	}

}
