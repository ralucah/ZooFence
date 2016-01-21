package ch.unine.zkexecutor;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class TCPServer extends Thread {
	
	private ServerSocket serverSocket;
	
	private ConcurrentHashMap<String, BufferedReader> inFromClient;
	private ConcurrentHashMap<String, DataOutputStream> outToClient;
	
	public TCPServer(String executorAddress) {
		int port = new Integer(executorAddress.split(":")[1]);
		
		try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		inFromClient = new ConcurrentHashMap<String, BufferedReader>();
		outToClient = new ConcurrentHashMap<String, DataOutputStream>();
	}
	
	@Override
	public void run() {
		while(true) {
			try {
				Socket connectionSocket = serverSocket.accept();
				
				// get input stream from client
				BufferedReader in = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream())); 
				
				// get output stream towards client
				DataOutputStream out = new DataOutputStream(connectionSocket.getOutputStream());
				
				String clientId = in.readLine();
				clientId = clientId.trim();
				
				inFromClient.put(clientId, in);
				outToClient.put(clientId, out);
				out.writeBytes("ACK");
				out.writeChar('\n');
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void replyToClient(String clientID, String cmdID, byte[] result) {		
		ClientReplyThread clientReplyT = new ClientReplyThread(outToClient.get(clientID), cmdID, result); 
		clientReplyT.start();
	}
}
