package hortonworks.hdp.apputil.registry;

import java.util.List;
import java.util.Map;


public interface HDPServiceRegistry {

	String getHBaseZookeeperHost() ;
	
	String getHBaseZookeeperClientPort() ;

	String getHBaseZookeeperZNodeParent() ;
	
	String getPhoenixConnectionURL() ;
	
	String getStormZookeeperQuorum() ;
	
	String getStormNimbusPort() ;
	
	String getStormNimbusHost() ;
	
	String getKafkaBrokerList() ;	
	
	List<String> getStormZookeeperQuorumAsList();	
	
	String getKafkaZookeeperHost() ;
	
	String getKafkaZookeeperClientPort() ;	
	
	String  getKafkaZookeeperZNodeParent() ;		
	
	String getHDFSUrl() ;
	
	String getHiveMetaStoreUrl();	
	
	String getHiveServer2ConnectionURL() ;
	
	String getFalconServerUrl() ;
	
	String getFalconBrokerUrl() ;
	
	String getFalconServerPort() ;
	
	String getAmbariServerUrl() ;
	
	String getOozieUrl() ;
	
	String getClusterName() ;
	
	
	String getResourceManagerURL() ;
	
	String getResourceManagerUIURL() ;

	String getStormUIUrl();

	Map<String, String> getRegistry();

	void populate(ServiceRegistryParams params) throws Exception;
	
	void writeToPropertiesFile() throws Exception;	

	
}
