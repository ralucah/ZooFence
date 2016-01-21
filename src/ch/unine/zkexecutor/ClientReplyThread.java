package ch.unine.zkexecutor;

import java.io.DataOutputStream;
import java.io.IOException;

public class ClientReplyThread extends Thread {
	private DataOutputStream out;
	private String cmdID;
	private byte[] result;
	
	public ClientReplyThread(DataOutputStream out, String cmdID, byte[] result) {
		this.out = out;
		this.cmdID = cmdID;
		this.result = result;
	}
	
	public void run() {
        try {
        	out.writeBytes(cmdID);
			out.writeChar('\n');
			out.writeInt(result.length);
			out.write(result);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
   }
}
