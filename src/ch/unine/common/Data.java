package ch.unine.common;
import org.apache.zookeeper.data.Stat;

import ch.unine.zkpartitioned.ZooKeeperPartitioned;

public class Data {
		private byte[] data;
		private Stat stat;
		
		public Data(byte[] data, Stat stat) {
			this.data = data;
			this.stat = stat;
		}

		public byte[] getData() {
			return data;
		}

		public void setData(byte[] data) {		
			this.data = data;
		}

		public Stat getStat() {
			return stat;
		}

		public void setStat(Stat stat) {
			this.stat = stat;
		}
	}
