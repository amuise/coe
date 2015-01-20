package hortonworks.hdp.apputil;

import hortonworks.hdp.apputil.registry.DeploymentMode;
import hortonworks.hdp.apputil.registry.HDPServiceRegistry;
import hortonworks.hdp.apputil.registry.HDPServiceRegistryImpl;
import hortonworks.hdp.apputil.registry.ServiceRegistryParams;
import hortonworks.hdp.apputil.slider.hbase.HBaseSliderUtilsTest;
import hortonworks.hdp.apputil.slider.storm.StormSliderUtilsTest;

public abstract class BaseUtilsTest {

	private static final String AMBARI_SERVER_URL = "http://centralregion01.cloud.hortonworks.com:8080";

	protected HDPServiceRegistry createHDPServiceRegistry() throws Exception {
		String serviceRegistryPropertyFileLocation = "/Users/gvetticaden/Dropbox/Hortonworks/Development/Git/hdp-app-utils/hdp-app-utils/src/test/resources/registry";
		String configFileName = "george-hdp-service-config.properties";
		
		HDPServiceRegistry serviceRegistry = new HDPServiceRegistryImpl(serviceRegistryPropertyFileLocation, configFileName);
		serviceRegistry.populate(createServiceRegistryParams());
		return serviceRegistry;
	}	
	
	private ServiceRegistryParams createServiceRegistryParams() {
		ServiceRegistryParams params = new ServiceRegistryParams();
		params.setAmbariUrl(AMBARI_SERVER_URL);
		params.setClusterName("centralregioncluster");
		
		params.setStormDeploymentMode(DeploymentMode.SLIDER);
		params.setStormSliderPublisherUrl(StormSliderUtilsTest.SLIDER_STORM_PUBLISHER_URL);
		
		params.setHbaseDeploymentMode(DeploymentMode.SLIDER);
		params.setHbaseSliderPublisherUrl(HBaseSliderUtilsTest.SLIDER_HBASE_PUBLISHER_URL);
		
		return params;
	}		
}
