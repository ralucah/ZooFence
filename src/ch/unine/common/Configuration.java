package ch.unine.common;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.zookeeper.ZooKeeper;

public abstract class Configuration {
	//public static List<String> zksConnectStrings;
	public static String zkAdminConnectString;
	public static List<String> zksConnectStrings;
	public static int flatteningFactor;
	public static int reductionFactor;
	public static String executorAddress;
	public static String executorLog;
	
	public static void setup() {
		//zksConnectStrings = new ArrayList<String>();
		
		try {
			FileInputStream fstream = new FileInputStream("zkpartitioned.config");
			
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

			String line;
			while ((line = br.readLine()) != null) {
				/* ignore comments */
				if (line.contains("#") == false) {
					if (line.contains("ZOOKEEPERS")) {
						zksConnectStrings = new ArrayList<String>();
						
						String regex = "\\s*\\bZOOKEEPERS\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						line = line.replace("[", "");
						line = line.replace("]", "");
						
						StringTokenizer tokenizer = new StringTokenizer(line);
					    while (tokenizer.hasMoreTokens()) {
					    	zksConnectStrings.add(tokenizer.nextToken());
					    }
					} else
					if (line.contains("ZKADMIN")) {
						String regex = "\\s*\\bZKADMIN\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						zkAdminConnectString = line;
					} else if (line.contains("FLATTENING_FACTOR")) {
						String regex = "\\s*\\bFLATTENING_FACTOR\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						flatteningFactor = new Integer(line);
					} else if (line.contains("REDUCTION_FACTOR")) {
						String regex = "\\s*\\bREDUCTION_FACTOR\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						reductionFactor = new Integer(line);
					} else if (line.contains("EXECUTOR_ADDRESS")) {
						String regex = "\\s*\\bEXECUTOR_ADDRESS\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						executorAddress = line;
					} else if (line.contains("EXECUTOR_LOG")) {
						String regex = "\\s*\\bEXECUTOR_LOG\\b\\s*";
						line = line.replaceAll(regex, "");
						line = line.replace("=", "");
						executorLog = line;
					}
				}
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
