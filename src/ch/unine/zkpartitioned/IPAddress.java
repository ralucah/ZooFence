package ch.unine.zkpartitioned;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

public class IPAddress {
 public String getIP (){
      try {
         Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
 
         while(e.hasMoreElements()) {
            NetworkInterface ni = (NetworkInterface) e.nextElement();
            //System.out.println("Net interface: "+ni.getName());
            
            if (ni.getName().equalsIgnoreCase("eth0")) {
            	//System.out.println("Net interface: "+ni.getName());
            	
            	Enumeration<InetAddress> e2 = ni.getInetAddresses();
            	 
                while (e2.hasMoreElements()){
                    InetAddress ip = (InetAddress) e2.nextElement();
                    if (ip instanceof Inet4Address)
                    	//System.out.println("IP address: "+ ip.toString());
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
}