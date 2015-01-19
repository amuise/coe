package hortonworks.hdp.apputil.hbase;


import hortonworks.hdp.apputil.registry.HDPServiceRegistry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

public class HBaseUtils {
	
	private HDPServiceRegistry registry;

	public HBaseUtils(HDPServiceRegistry registry) {
		this.registry = registry;
	}
	
	public void createHBaseTable(String tableName, String columnFamily) throws Exception {
		Configuration config = constructConfiguration();
		HConnection connection = HConnectionManager.createConnection(config);
		HBaseAdmin admin = createHBaseAdmin(config);
		
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor meta = new HColumnDescriptor(columnFamily.getBytes());
		desc.addFamily(meta);
		admin.createTable(desc);	
		
		connection.close();
	}
	
	private HBaseAdmin createHBaseAdmin(Configuration config) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(config);
		return admin;
	}

	private Configuration constructConfiguration() throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum",
				registry.getHBaseZookeeperHost());
		config.set("hbase.zookeeper.property.clientPort",registry.getHBaseZookeeperClientPort());
		config.set("zookeeper.znode.parent", registry.getHBaseZookeeperZNodeParent());
		return config;
	}	

}
