package hortonworks.hdp.apputil.registry;


import java.util.List;

import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtilsTest;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtilsTest;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HDPServiceRegistryTest {
	
	private static final String REGISTRY_CONFIG_LOCATION = "/Users/gvetticaden/Dropbox/Hortonworks/Development/Workspaces/main/hdp-app-utils/src/test/resources/registry/hdp-service-config.properties";

	@Test
	public void tesetPopulateRegistryFromFile() {
		HDPServiceRegistry registry = constructHDPServiceRegistry();
	
		assertThat(registry.getRegistry().size(), is(24));
		assertThat(registry.getAmbariServerUrl(), is(""));
		assertThat(registry.getFalconServerPort(), is("15000"));
	}
	
	@Test
	public void tesetPopulateRegistryFromRelativeFile() {
		HDPServiceRegistry registry = new HDPServiceRegistryImpl("registry/hdp-service-config.properties");
	
		assertThat(registry.getRegistry().size(), is(24));
		assertThat(registry.getAmbariServerUrl(), is(""));
		assertThat(registry.getFalconServerPort(), is("15000"));
	}	
	
	@Test
	public void testPopulateRegistryFromAmbariAndSliderHBaseAndStorm() throws Exception{
	
		HDPServiceRegistry registry = constructHDPServiceRegistry();
		
		ServiceRegistryParams params = createServiceRegistryParams();
		
		//populate
		registry.populate(params);
		
		//do asserts
		testEntireRegistry(registry);
	}
	
	public void testEntireRegistry(HDPServiceRegistry serviceRegistry) {
		
		assertThat(serviceRegistry.getFalconServerUrl(),  is("http://centralregion03.cloud.hortonworks.com:15000"));
		
		assertThat(serviceRegistry.getHBaseZookeeperClientPort() ,  is("2181"));
		assertThat(serviceRegistry.getHBaseZookeeperHost(), is("centralregion01.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getHBaseZookeeperZNodeParent(), is("/services/slider/users/yarn/hbase-on-yarn-v30"));
		
		assertThat(serviceRegistry.getHDFSUrl(), is("hdfs://centralregion01.cloud.hortonworks.com:8020"));
		
		assertThat(serviceRegistry.getHiveMetaStoreUrl() , is("thrift://centralregion03.cloud.hortonworks.com:9083"));
		assertThat(serviceRegistry.getHiveServer2ConnectionURL() , is("jdbc:hive2://centralregion03.cloud.hortonworks.com:10000"));

		assertThat(serviceRegistry.getKafkaBrokerList()  , is("centralregion01.cloud.hortonworks.com:6667,centralregion02.cloud.hortonworks.com:6667"));
		assertThat(serviceRegistry.getKafkaZookeeperClientPort(), is("2181"));
		assertThat(serviceRegistry.getKafkaZookeeperHost(), is("centralregion01.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getKafkaZookeeperZNodeParent(), is(""));
		
		assertThat(serviceRegistry.getStormNimbusHost(), is("centralregion10.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getStormNimbusPort(), is("52110"));
		assertThat(serviceRegistry.getStormZookeeperQuorum(), is("centralregion01.cloud.hortonworks.com,centralregion02.cloud.hortonworks.com,centralregion03.cloud.hortonworks.com"));
		assertThat(serviceRegistry.getStormUIUrl() , is("http://centralregion08.cloud.hortonworks.com:54127"));

		
		List<String> zookeepers = serviceRegistry.getStormZookeeperQuorumAsList();
		assertThat(zookeepers.size(), is(3));
		for(String zookeeper: zookeepers) {
			System.out.println(zookeeper);
		}		

		assertThat(serviceRegistry.getPhoenixConnectionURL(), is("jdbc:phoenix:centralregion01.cloud.hortonworks.com:2181:/services/slider/users/yarn/hbase-on-yarn-v30"));

		assertThat(serviceRegistry.getClusterName(),  is("centralregioncluster"));
		
		assertThat(serviceRegistry.getAmbariServerUrl(),  is("http://centralregion01.cloud.hortonworks.com:8080"));

		assertThat(serviceRegistry.getResourceManagerURL(), is("centralregion02.cloud.hortonworks.com:8050"));
		assertThat(serviceRegistry.getResourceManagerUIURL() , is("http://centralregion02.cloud.hortonworks.com:8088"));
		
		
		assertThat(serviceRegistry.getOozieUrl() , is("http://centralregion03.cloud.hortonworks.com:11000/oozie"));
		
		//assertThat(serviceRegistry.getActiveMQConnectionUrl(), is("tcp://george-activemq01.cloud.hortonworks.com:61616?wireFormat.maxInactivityDuration=0"));
		//assertThat(serviceRegistry.getActiveMQHost(),  is("george-activemq01.cloud.hortonworks.com"));
		//assertThat(serviceRegistry.getMailSMTPHost(), is("hadoopsummit-stormapp.secloud.hortonworks.com"));	
		//assertThat(serviceRegistry.getHueServerUrl(), is("http://centralregion10.cloud.hortonworks.com:8000"));
		//assertThat(serviceRegistry.getRangerServerUrl() , is("http://george-security01.cloud.hortonworks.com:6080/"));
		
		//assertThat(serviceRegistry.getActiveMQAdminConsoleUrl() , is("http://george-activemq01.cloud.hortonworks.com:8161/admin/topics.jsp"));
		//assertThat(serviceRegistry.getSolrAdminUrl() , is("http://george-search01.cloud.hortonworks.com:8983/solr/"));
		//assertThat(serviceRegistry.getSolrBananaDashboardCompletedUrl() , is("http://george-search01.cloud.hortonworks.com:8983/banana/#/dashboard/solr/Truck%20Events%20Dashboard%20V4"));
		//assertThat(serviceRegistry.getSolrBananaDashboardEmptyUrl() , is("http://george-search01.cloud.hortonworks.com:8983/banana/#/dashboard/solr/Demo%20Trucking%20Events%20Dashboard%20V3"));
		//assertThat(serviceRegistry.getSolrDeleteTruckIndexUrl() , is("http://george-search01.cloud.hortonworks.com:8983/solr/truck_event_logs/update?stream.body=%3Cdelete%3E%3Cquery%3Eid:*%3C/query%3E%3C/delete%3E&commit=true"));
		//assertThat(serviceRegistry.getSolrIndexPigJobHueUrl() , is("http://stormapp01.cloud.hortonworks.com:8000/pig/3"));
		//assertThat(serviceRegistry.getSolrServerUrl(), is("http://george-search01.cloud.hortonworks.com:8983/solr"));		
		
	}	
	
	private HDPServiceRegistry constructHDPServiceRegistry() {
		String serviceRegistryPropertyFileLocation = REGISTRY_CONFIG_LOCATION;		
		HDPServiceRegistry registry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation);
		return registry;
	}

	private ServiceRegistryParams createServiceRegistryParams() {
		ServiceRegistryParams params = new ServiceRegistryParams();
		params.setAmbariUrl("http://centralregion01.cloud.hortonworks.com:8080");
		params.setClusterName("centralregioncluster");
		
		params.setHbaseDeploymentMode(DeploymentMode.SLIDER);
		params.setHbaseSliderPublisherUrl(HBaseSliderUtilsTest.SLIDER_HBASE_PUBLISHER_URL);
		params.setStormDeploymentMode(DeploymentMode.SLIDER);
		params.setStormSliderPublisherUrl(StormSliderUtilsTest.SLIDER_STORM_PUBLISHER_URL);
		
//		params.setActiveMQConnectionUrl("tcp://george-activemq01.cloud.hortonworks.com:61616");
//		params.setActiveMQHost("george-activemq01.cloud.hortonworks.com");
//		
//		params.setMailSmtpHost("hadoopsummit-stormapp.secloud.hortonworks.com");
//		params.setSolrServerUrl("http://george-search01.cloud.hortonworks.com:8983/solr");
//		
//
//		params.setHueServerUrl("http://centralregion10.cloud.hortonworks.com:8000");
//		params.setRangerServerUrl("http://george-security01.cloud.hortonworks.com:6080/");
//		params.setActiveMQConsoleUrl("http://george-activemq01.cloud.hortonworks.com:8161/admin/topics.jsp");
//		params.setSolrAdminUrl("http://george-search01.cloud.hortonworks.com:8983/solr/");
//		params.setBananaDashboardCompletedUrl("http://george-search01.cloud.hortonworks.com:8983/banana/#/dashboard/solr/Truck%20Events%20Dashboard%20V4");
//		params.setBananaDashboardEmptyUrl("http://george-search01.cloud.hortonworks.com:8983/banana/#/dashboard/solr/Demo%20Trucking%20Events%20Dashboard%20V3");
//		params.setDeleteTruckIndexUrl("http://george-search01.cloud.hortonworks.com:8983/solr/truck_event_logs/update?stream.body=%3Cdelete%3E%3Cquery%3Eid:*%3C/query%3E%3C/delete%3E&commit=true");
//		params.setSolrIndexPigJobHueUrl("http://stormapp01.cloud.hortonworks.com:8000/pig/3");
		return params;
	}		

}
