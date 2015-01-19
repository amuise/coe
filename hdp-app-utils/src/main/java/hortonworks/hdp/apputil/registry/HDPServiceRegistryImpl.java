package hortonworks.hdp.apputil.registry;

import hortonworks.hdp.apputil.ambari.AmbariUtils;
import hortonworks.hdp.apputil.registry.RegistryKeys;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtils;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.antlr.grammar.v3.ANTLRParser.finallyClause_return;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


public class HDPServiceRegistryImpl implements HDPServiceRegistry{

	private static final Logger LOG = Logger.getLogger(HDPServiceRegistryImpl.class);
	
	private Map<String, String> registry = new HashMap<String, String>();
	private String hdpServiceRegistryConfigLocation;
	
	public HDPServiceRegistryImpl(String configFileLocation) {
		this.hdpServiceRegistryConfigLocation = configFileLocation;
		this.populateRegistryFromConfigFile();
	}
	
	public void populateRegistryFromConfigFile()   {
		Properties hdpServiceConfigProperties = null;
		InputStream inputStream = null;
		try {
			hdpServiceConfigProperties = new Properties();
			inputStream = createConfigInputStream();
			hdpServiceConfigProperties.load(inputStream);
			LOG.info("Initial Load from Config File is: " + hdpServiceConfigProperties);
		} catch (Exception e) {
			String errorMsg = "Encountered error while reading configuration properties: "
					+ e.getMessage();
			LOG.error(errorMsg);
			throw new RuntimeException(errorMsg, e);
		} finally {
			if(inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					LOG.error("Error closing inputStream");
				}
			}
		}
		Set<Object> keys = hdpServiceConfigProperties.keySet();
		for(Object propertyKey: keys) {
			String key = (String)propertyKey;
			String value = hdpServiceConfigProperties.getProperty(key);
			saveToRegistry(key, value);
		}
	}


	public void populate(ServiceRegistryParams params) throws Exception {
		
		saveToRegistry(RegistryKeys.AMBARI_SERVER_URL, params.getAmbariUrl());
		saveToRegistry(RegistryKeys.AMBARI_CLUSTER_NAME, params.getClusterName());
		
		String ambariRestUrl = constructAmbariRestURL(params.getAmbariUrl(), params.getClusterName());
		AmbariUtils ambariService = new AmbariUtils(ambariRestUrl);
		
		populateRegistryEndpointsFromAmbari(ambariService);
		populateRegistryEndpointsForHBase(ambariService, params);
		populateRegistryEndpointsForStorm(ambariService, params);
		
	}
	
	public void populateRegistryEndpointsFromAmbari(ServiceRegistryParams params) throws Exception{

		String ambariRestUrl = constructAmbariRestURL(params.getAmbariUrl(), params.getClusterName());
		AmbariUtils ambariService = new AmbariUtils(ambariRestUrl);		
		
		populateRegistryEndpointsFromAmbari(ambariService);
	}

	private void populateRegistryEndpointsFromAmbari(AmbariUtils ambariService) throws Exception {
		LOG.info("Starting to populate Service Registry from Ambari");
		
		//falcon configuration
		String falconSeverUrl = "http://"+ambariService.getFalconHost() + ":" + getValueForKey(RegistryKeys.FALCON_SERVER_PORT);
		saveToRegistry(RegistryKeys.FALCON_SERVER_URL , falconSeverUrl);
		saveToRegistry(RegistryKeys.FALCON_BROKER_URL , ambariService.getFalconBrokerUrl());
		
		//HDFS configuration
		saveToRegistry(RegistryKeys.HDFS_URL , ambariService.getHDFSUrl());

		//YARN configuration
		saveToRegistry(RegistryKeys.RESOURCE_MANGER_URL, ambariService.getResourceManagerUrl());
		saveToRegistry(RegistryKeys.RESOURCE_MANGER_URI_URL, ambariService.getResourceManagerUIUrl());
		
		
		//Hive Configuration
		saveToRegistry(RegistryKeys.HIVE_METASTORE_URL, ambariService.getHiveMetaStoreUrl());
		String hiveServer2ConnectString = "jdbc:hive2://"+ambariService.getHiveServer2Host() +":"+ambariService.getHiveServer2ThriftPort();
		saveToRegistry(RegistryKeys.HIVE_SERVER2_CONNECT_URL, hiveServer2ConnectString);
		
	
		//Oozie Configuration
		saveToRegistry(RegistryKeys.OOZIE_SERVER_URL, ambariService.getOozieServerUrl());
		

		//Kafka Configuration
		
		List<String> kafkaBrokerHosts = ambariService.getKafkaBrokerList();
		String kafkaBrokerPort = ambariService.getKafkaBrokerPort();
		StringBuffer kafkaBrokerListBuffer = new StringBuffer();
		boolean isFirst = true;
		for(String kafkaBrokerHost: kafkaBrokerHosts) {
			if(!isFirst) {
				kafkaBrokerListBuffer.append(",");
			}
			kafkaBrokerListBuffer.append(kafkaBrokerHost).append(":").append(kafkaBrokerPort);
			isFirst = false;
		}
		
		saveToRegistry(RegistryKeys.KAFKA_BROKER_LIST, kafkaBrokerListBuffer.toString());
		
			
		String zookeeperConnectString = ambariService.getKafkaZookeeperConnect();
		String[] zookeepers = zookeeperConnectString.split(",");
		if(zookeepers.length > 0) {
			String[] zookeeperHostPort = zookeepers[0].split(":");
			if(zookeeperHostPort.length == 2) {
				saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_HOST, zookeeperHostPort[0]);
				saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_CLIENT_PORT, zookeeperHostPort[1]);
			} else {
				LOG.error("KafkaBrokerString["+zookeepers[0] + "] is not in the right format");
			}
			
		} else {
			LOG.error("Kafka Zookeeper connect string["+zookeeperConnectString+"] is not in right format");
		}
		
		
		//TODO: Not sure where to get this value. Can't find it in Ambari
		saveToRegistry(RegistryKeys.KAFKA_ZOOKEEPER_ZNODE_PARENT, "");
		
		LOG.info("Finished Populating Service Registry from Ambari");
	}	
	
	private void populateRegistryEndpointsForHBase(AmbariUtils ambariService, ServiceRegistryParams params) throws Exception  {
		
		DeploymentMode hbaseDeploymentMode = params.getHbaseDeploymentMode();
		
		if(hbaseDeploymentMode != null) {
			
			String hbaseZookeeperHost = null;
			String phoenixConnectionUrl = null;
			String hBaseZookeeperParentNode = null;
			String hBaseZookepperCientPort = null;
			
			if(DeploymentMode.SLIDER.equals(hbaseDeploymentMode)) {
				
				String hbaseSliderPublisherUrl = params.getHbaseSliderPublisherUrl();
				if(StringUtils.isEmpty(hbaseSliderPublisherUrl)) {
					String errMsg = "hbaseSliderPublisherUrl is required";
					LOG.error(errMsg);
					throw new Exception(errMsg);
				}
				
				HBaseSliderUtils hbaseSliderService = new HBaseSliderUtils(hbaseSliderPublisherUrl);
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating HBase endpoints from HBase Slider Service");
				}
				
				hBaseZookeeperParentNode = hbaseSliderService.getHBaseZookeeperParentNode();
				hBaseZookepperCientPort = hbaseSliderService.getHBaseZookepperCientPort();
				List<String> hbaseZookeeperQuorumList = hbaseSliderService.getHBaseZookeeperQuorum();
				hbaseZookeeperHost = hbaseZookeeperQuorumList.get(0);	
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating HBase endpoints from HBaseSliderUtils");
				}				
				
			} else if(DeploymentMode.STANDALONE.equals(hbaseDeploymentMode)) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating HBase endpoints from Ambari");
				}
				
				hBaseZookeeperParentNode = ambariService.getHBaseZookeeperParentNode();
				hBaseZookepperCientPort = ambariService.getHBaseZookepperCientPort();
				List<String>  hbaseZookeeperQuorumList = ambariService.getHBaseZookeeperQuorum();				
				hbaseZookeeperHost = hbaseZookeeperQuorumList.get(0);	
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating HBase endpoints from Ambari");
				}			
				
			} 
			
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_HOST , hbaseZookeeperHost);	
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_ZNODE_PARENT , hBaseZookeeperParentNode);
			saveToRegistry(RegistryKeys.HBASE_ZOOKEEPER_CLIENT_PORT , hBaseZookepperCientPort);	
			/* From the HBase info, we can create phoenixConnectionUrl */
			phoenixConnectionUrl = "jdbc:phoenix:"+hbaseZookeeperHost+ ":" + hBaseZookepperCientPort + ":" + hBaseZookeeperParentNode;
			saveToRegistry(RegistryKeys.PHOENIX_CONNECTION_URL, phoenixConnectionUrl);
					
		} else {
			LOG.info("Deployment Mode for HBase not configured. Skipping loading HBase Registry values.");
		}
		
	}
	
	private void populateRegistryEndpointsForStorm(AmbariUtils ambariService, ServiceRegistryParams params) throws Exception  {
		
		DeploymentMode stormDeploymentMode = params.getStormDeploymentMode();
		
		if(stormDeploymentMode != null) {
			String stormNimbusHost = null;
			String stormNimbusPort = null;
			String stormUIServer = null;
			String stormUIPort = null;
			String rawStormZookeeperQuorum = null;
			
			if(DeploymentMode.SLIDER.equals(stormDeploymentMode)) {
				
				String stormSliderPublisherUrl = params.getStormSliderPublisherUrl();
				if(StringUtils.isEmpty(stormSliderPublisherUrl)) {
					String errMsg = "stormSliderPublisherUrl is required";
					LOG.error(errMsg);
					throw new Exception(errMsg);
				}
				StormSliderUtils stormSliderService = new StormSliderUtils(stormSliderPublisherUrl);
		
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating Storm endpoints from Storm Slider Service");
				}
				
				stormNimbusHost = stormSliderService.getStormNimbusHost();
				stormNimbusPort = stormSliderService.getStormNimbusPort();
				stormUIServer = stormSliderService.getStormUIServer();
				stormUIPort = stormSliderService.getStormUIPort();
				rawStormZookeeperQuorum = stormSliderService.getStormZookeeperQuorum();
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating Storm endpoints from StormSliderUtils");
				}				
				
			} else if (DeploymentMode.STANDALONE.equals(stormDeploymentMode)) {
				
				if(LOG.isInfoEnabled()) {
					LOG.info("Populating Storm endpoints from Ambari");
				}
				
				stormNimbusHost = ambariService.getStormNimbusHost();
				stormNimbusPort = ambariService.getStormNimbusPort();
				stormUIServer = ambariService.getStormUIServer();
				stormUIPort = ambariService.getStormUIPort();
				rawStormZookeeperQuorum =  ambariService.getStormZookeeperQuorum();
							

				if(LOG.isInfoEnabled()) {
					LOG.info("Finished Populating Storm endpoints from Ambari");
				}			
				
			}
			
			saveToRegistry(RegistryKeys.STORM_NIMBUS_HOST, stormNimbusHost);
			saveToRegistry(RegistryKeys.STORM_NIMBUS_PORT , stormNimbusPort);
			saveToRegistry(RegistryKeys.STORM_UI_HOST, stormUIServer);
			saveToRegistry(RegistryKeys.STORM_UI_PORT, stormUIPort);

			String parsedStormZookeeperQuorum = rawStormZookeeperQuorum.replace("['", "").replace("']","").replace("'", "");
			saveToRegistry(RegistryKeys.STORM_ZOOKEEPER_QUORUM , parsedStormZookeeperQuorum);				
			
		} else {
			LOG.info("Deployment Mode for Storm not configured. Skipping loading Storm Registry values.");
		}
	}		

	
	public Map<String, String> getRegistry() {
		return registry;
	}
	

	
	public String getHBaseZookeeperHost() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_HOST);
	}	
	
	public String getHBaseZookeeperClientPort() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_CLIENT_PORT);
	}	

	public String getHBaseZookeeperZNodeParent() {
		return getValueForKey(RegistryKeys.HBASE_ZOOKEEPER_ZNODE_PARENT);
	}	
	
	public String getPhoenixConnectionURL() {
		return getValueForKey(RegistryKeys.PHOENIX_CONNECTION_URL);
	}
	
	public String getStormZookeeperQuorum() {
		return getValueForKey(RegistryKeys.STORM_ZOOKEEPER_QUORUM);
	}
	
	public String getStormNimbusPort() {
		return getValueForKey(RegistryKeys.STORM_NIMBUS_PORT);
	}	
	
	public String getStormNimbusHost() {
		return getValueForKey(RegistryKeys.STORM_NIMBUS_HOST);
	}	
	
	public String getKafkaBrokerList() {
		return getValueForKey(RegistryKeys.KAFKA_BROKER_LIST);
	}		
	
	public List<String> getStormZookeeperQuorumAsList() {
		List<String> stormQuorumList = new ArrayList<String>();
		String zooKeeperQuorum =  getValueForKey(RegistryKeys.STORM_ZOOKEEPER_QUORUM);
		if(StringUtils.isNotEmpty(zooKeeperQuorum)) {
			String[] quorumArray = zooKeeperQuorum.split(",");
			stormQuorumList = Arrays.asList(quorumArray);
		}
		return stormQuorumList;
	}		
	
	public String getKafkaZookeeperHost() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_HOST);
	}	
	
	public String getKafkaZookeeperClientPort() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_CLIENT_PORT);
	}		
	
	public String  getKafkaZookeeperZNodeParent() {
		return getValueForKey(RegistryKeys.KAFKA_ZOOKEEPER_ZNODE_PARENT);
	}		
	
	public String getHDFSUrl() {
		return getValueForKey(RegistryKeys.HDFS_URL);
	}	
	
	public String getHiveMetaStoreUrl() {
		return getValueForKey(RegistryKeys.HIVE_METASTORE_URL);
	}	
	
	public String getHiveServer2ConnectionURL() {
		return getValueForKey(RegistryKeys.HIVE_SERVER2_CONNECT_URL);
	}	
	


	public String getFalconServerUrl() {
		return getValueForKey(RegistryKeys.FALCON_SERVER_URL);
	}	
	
	public String getFalconBrokerUrl() {
		return getValueForKey(RegistryKeys.FALCON_BROKER_URL);
	}
	
	public String getFalconServerPort() {
		return getValueForKey(RegistryKeys.FALCON_SERVER_PORT);
	}		
	
	public String getAmbariServerUrl() {
		return getValueForKey(RegistryKeys.AMBARI_SERVER_URL);
	}
	
	public String getOozieUrl() {
		return getValueForKey(RegistryKeys.OOZIE_SERVER_URL);
	}	
	
	public String getClusterName() {
		return getValueForKey(RegistryKeys.AMBARI_CLUSTER_NAME);
	}	
	

	
	public String getResourceManagerURL() {
		return getValueForKey(RegistryKeys.RESOURCE_MANGER_URL);
	}
	
	public String getResourceManagerUIURL() {
		String url = null;
		String hostAndPort = getValueForKey(RegistryKeys.RESOURCE_MANGER_URI_URL);
		if(StringUtils.isNotEmpty(hostAndPort)) {
			url = "http://"+hostAndPort;
		}
		return url;
	}	
	
	
	public String getStormUIUrl() {
		String stormUIHost = getValueForKey(RegistryKeys.STORM_UI_HOST);
		String stormUIPort = getValueForKey(RegistryKeys.STORM_UI_PORT);
		String url = null;
		if(StringUtils.isNotEmpty(stormUIHost) && StringUtils.isNotEmpty(stormUIPort)) {
			url = "http://"+stormUIHost + ":" + stormUIPort;
		}
		return url;
	}


	protected String getValueForKey(String keyName) throws RuntimeException {
		String value = registry.get(keyName);
		if(LOG.isDebugEnabled()) {
			LOG.debug("Value for key["+keyName + "] is:  " + value);
		}
		return value;
	}
	
	private String constructAmbariRestURL(String ambariUrl, String clusterName) {
		StringBuffer buffer = new StringBuffer();
		buffer.append(ambariUrl)
			  .append("/api/v1/clusters/")
			  .append(clusterName);
		return buffer.toString();
	}	
	
	public void saveToRegistry(String key, String value) {
		if(key!= null) {
			registry.put(key, value);			
		} else {
			LOG.error("Skipping persisting key["+key + "] into service registry because it is null");
		}	
	}

	private InputStream createConfigInputStream() throws FileNotFoundException {
		InputStream inputStream;
		if(this.hdpServiceRegistryConfigLocation.startsWith("/")) {
			inputStream = new FileInputStream(this.hdpServiceRegistryConfigLocation);
		} else {
			inputStream = this.getClass().getClassLoader().getResourceAsStream(this.hdpServiceRegistryConfigLocation);
		}
		return inputStream;
	}	
		
	

	
}
