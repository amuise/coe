package hortonworks.hdp.apputil.registry;


import java.util.List;

import hortonworks.hdp.apputil.BaseUtilsTest;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtilsTest;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtilsTest;

import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class HDPServiceRegistryTest extends BaseUtilsTest {
	
	
	@Test
	public void tesetPopulateRegistryFromFile() throws Exception {
		HDPServiceRegistry registry = createHDPServiceRegistry();
	
		assertThat(registry.getRegistry().size(), is(24));
		assertThat(registry.getAmbariServerUrl(), is("http://centralregion01.cloud.hortonworks.com:8080"));
		assertThat(registry.getFalconServerPort(), is("15000"));
	}
	
	@Test
	public void testPopulateRegistryFromAmbariAndSliderHBaseAndStorm() throws Exception{
	
		HDPServiceRegistry registry = createHDPServiceRegistry();
		//do asserts
		testEntireRegistry(registry);
	}
	
	@Test
	public void testWritingRegistryToFile() throws Exception {
		HDPServiceRegistry registry = createHDPServiceRegistry();
		registry.writeToPropertiesFile();
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
	}	
	
	
}
