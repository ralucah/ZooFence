package ch.unine.common;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.zookeeper.ZooKeeper;

import ch.unine.common.Command.CmdType;

public class MappingFunction {
	private List<ZooKeeper> zookeepers;
	private int flatteningFactor; /* when path length divisible with flattening factor, remove zookeepers */
	private int reductionFactor;  /* number of zookeepers to remove */
	
	private Map<String, Integer> ipToZk = new HashMap<String, Integer>() {{
		put("/127.0.0.1", 0);
        //put("/10.0.13.110", 0);
		//put("/10.0.13.111", 1);
		//put("/10.0.13.112", 2);
		//put("/10.0.13.113", 3);
	}};
	
	public MappingFunction(List<ZooKeeper> zookeepers, int flatteningFactor, int reductionFactor) {
		this.zookeepers = zookeepers;
		this.flatteningFactor = flatteningFactor;
		this.reductionFactor = reductionFactor;
	}
	
	private String getIP() {
        try {
        	Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
 
        	while(e.hasMoreElements()) {
        		NetworkInterface ni = (NetworkInterface) e.nextElement();
	    
        		if (ni.getName().equalsIgnoreCase("eth0")) {
        			Enumeration<InetAddress> e2 = ni.getInetAddresses();
	    	 
        			while (e2.hasMoreElements()){
        				InetAddress ip = (InetAddress) e2.nextElement();
        				if (ip instanceof Inet4Address)
        					return ip.toString();
        			}
        		}
        	}
        }
        catch (Exception e) {
        	e.printStackTrace();
        }
        return null;
	}
	
	// builds list of zks
	private void computePartitions(String path, List<ZooKeeper> zks) {
		String[] pathTokens = path.split("/");
		int pathTokensLen = pathTokens.length - 1;
		
		if (pathTokensLen > 1) {
				String parentPath = "";
				
				for (int i = 1 ; i < pathTokensLen; i++) { // skip first token since it's space
					parentPath += "/".concat(pathTokens[i]);
				}
				
				computePartitions(parentPath,zks);
				
				if (pathTokensLen % flatteningFactor == 0) {
					int reductionCount = 0;
					int indexToRemove;
					
					while(zks.size() > 1 && reductionCount < reductionFactor) {
						indexToRemove = path.hashCode() % zks.size();
						zks.remove(Math.abs(indexToRemove));
						reductionCount++;
					}
				}
			}
	}
	
	// builds log key
	private String computeKey(List<ZooKeeper> zks) {
		String logKey = "";
		
		for (ZooKeeper zk : zks) {
			logKey += zookeepers.indexOf(zk);
		}
		
		return logKey;
	}
	
	/*public void map(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {	
		// generate partitions
		List<ZooKeeper> zksClone = new ArrayList<ZooKeeper>(zookeepers);
		computePartitions(cmd.getPath(), zksClone);
		for (ZooKeeper zk : zksClone) {
			zks.add(zk);
		}

		// log key
		//if (zks.size() > 1)
		logKeys.add(computeKey(zksClone));
		
		// parent log key
		if (cmd.getType().equals(CmdType.CREATE) && cmd.getPathLength() % flatteningFactor == 0) {
			String parentPath = cmd.getParentPath();
			
			if (!parentPath.equals("/")) {
				zksClone = new ArrayList<ZooKeeper>(zookeepers);
				computePartitions(parentPath, zksClone);
				
				if (zksClone.size() > 1)
					logKeys.add(computeKey(zksClone));
			}
		}
	}*/
	
	public void map(Command cmd, List<ZooKeeper> zks, List<String> logKeys) {
		String path = cmd.getPath();
		char digit = 0;
		
		for (int i = 0 ; i < path.length() && digit == 0; i++) {
			if (Character.isDigit(path.charAt(i))) {
				digit = path.charAt(i);
			}
		}
		
		if (Character.isDigit(digit)) {
			
			int zkNumber = digit - '0';
			zks.add(zookeepers.get(zkNumber % zookeepers.size()));
			
			logKeys.add(((Integer)zkNumber).toString());
		} else {
			if (cmd.getType().equals(CmdType.GET_DATA)) {
				int zkNumber = ipToZk.get(this.getIP()).intValue(); //(new Random()).nextInt(zookeepers.size());
				zks.add(zookeepers.get(zkNumber));
				logKeys.add(((Integer)zkNumber).toString());
			} else {
				String key = "";
				for (ZooKeeper z : zookeepers) {
					zks.add(z);
					key += ((Integer)zookeepers.indexOf(z)).toString();
				}
				logKeys.add(key);
			}
		}
	}
	
	public List<ZooKeeper> getZks(String key) {
		List<ZooKeeper> zks = new ArrayList<ZooKeeper>();
		for (char ch : key.toCharArray()) {
			int index = Character.getNumericValue(ch);
			zks.add(zookeepers.get(index));
		}
		return zks;
	}
}
