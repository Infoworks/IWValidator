package io.infoworks.ingestion;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.source.SourceDriverName;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.source.SourceType;
import io.infoworks.ingestion.tgt.TargetInfo;

public class SourceIngestTest {

	private static MetadataInfo mi ; 
	private static SourceInfo si ;
	private static TargetInfo ti ;
	private static String suff ;
	private Logger logger = Logger.getLogger("SourceIngestTest") ;
	
	@Before
	public void setup() { 
		init();
	}
	
	@Test
	public void testSrc() {
		BaseMongoManager msetup = new BaseMongoManager(mi);
		msetup.insert(si, ti, suff);
	}

	private void init() {
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
			suff = new Date().toString().replaceAll(":","_").replaceAll(" ", "_") ;
			String srcname = "automatedtest_" + suff ;
			
			si = new RDBMSSouceInfo(
					System.getProperty("src_hostname"), 
					//Integer.getInteger(System.getProperty("src_port")), 
					1521, //TODO - Change
					System.getProperty("src_schema"), 
					System.getProperty("src_userName"), 
					System.getProperty("src_password"), 
					SourceDriverName.ORACLE_DRIVER_NAME) ; //TODO - Refactor for other Dbs
			si.setStype(SourceType.ORACLE); //TODO - Changes
			si.setName(srcname);
			logger.info("got source info:" + si);
		}
		if(ti == null) { 
			TargetInfo ti = new TargetInfo() ;
			ti.setHdfsPath(System.getProperty("tgt_hdfsPath"));
			ti.setHiveSchema(System.getProperty("tgt_hiveSchema"));
			logger.info("got target info: " + ti) ;
		}
	}
}
