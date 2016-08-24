package io.infoworks.ingestion;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;

import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.source.SourceDriverName;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.source.SourceType;
import io.infoworks.ingestion.tgt.TargetInfo;
import io.infoworks.util.HdfsUtil;

public class SourceIngestTest {
/*
	private static MetadataInfo mi ; 
	private static SourceInfo si ;
	private static TargetInfo ti ;
	private static String suff  ;
	private Logger logger = Logger.getLogger("SourceIngestTest") ;
	
	@Before
	public void setup() { 
		init();
	}
	
	@Test
	public void testSrc() {
		Base msetup = new Base(mi);
		msetup.insert(si, ti, suff);
		
		msetup.deleteSource(msetup.findSource(si.getName()), ti);
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
			ti = new TargetInfo() ;
			ti.setHdfsHost(System.getProperty("tgt_hdfsHost"));
			ti.setHdfsPath(System.getProperty("tgt_hdfsPath"));
			ti.setHiveSchema(System.getProperty("tgt_hiveSchema"));
			String portstr =   System.getProperty("tgt_hdfsPort");
			int portint = new Integer(portstr).intValue() ;
			logger.info("port int:" + portint);
			ti.setHdfsPort(portint);
			logger.info("got target info: " + ti) ;
		}
	}*/
	
	
}
