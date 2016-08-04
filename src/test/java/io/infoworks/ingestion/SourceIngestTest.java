package io.infoworks.ingestion;

import static org.junit.Assert.*;

import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.tgt.TargetInfo;

public class SourceIngestTest {

	private static MetadataInfo mi ; 
	private static SourceInfo si ;
	private static TargetInfo ti ;
	private Logger logger = Logger.getLogger("SourceIngestTest") ;
	
	@Before
	public void setup() { 
		getMetaInfo();
	}
	
	@Test
	public void testSrc() {
		BaseMongoManager msetup = new BaseMongoManager(mi);
		
	}

	private void getMetaInfo() {
		if(mi == null) { 
			mi = new MetadataInfo() ;
			mi.setCollectionName(System.getProperty("meta_src_collectionName"));
			mi.setDbName(System.getProperty("meta_dbName"));
			mi.setHostName(System.getProperty("meta_hostName"));
			mi.setPassword(System.getProperty("meta_password"));
			if(mi.getPort() == 0) { 
				mi.setPort(27017);
			}
			else {
				mi.setPort(Integer.getInteger(System.getProperty("meta_port")));
			}
			
			mi.setUserId(System.getProperty("meta_userId"));
			logger.info("Created metadata:" + mi);
			
		}
		if(si == null) { 
					
		}
		if(ti == null) { 
	
		}
	}
}
