package io.infoworks.ingestion.services;


import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import org.json.simple.JSONObject;

import com.mongodb.BasicDBObject;

import io.infoworks.ingestion.metadata.Credentials;
import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.model.TableGroupInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.source.SourceDriverName;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.source.SourceType;
import io.infoworks.ingestion.tgt.TargetInfo;
import io.infoworks.util.JsonUtil;
import io.infoworks.util.PropertiesUtil;
import io.infoworks.util.RestPropertiesUtil;

public class IngestionHelper {

	private JsonUtil jsonUtil = new JsonUtil() ;
	
	private SourceService sourceService =  null; 
	private PropertiesUtil pu ;
	private Logger logger = Logger.getLogger("IngestionService") ;
	private List<String> insertedIds = new ArrayList<String>() ;
	
	public IngestionHelper() { 
		pu = PropertiesUtil.getInstance() ;
	}
	
	public void parseFiles() throws Exception {
		//TODP - make this configurable
		File directory = new File("/Users/sandeepkhurana/automation/IWValidator/src/main/resources/sources") ;
		// Get the files from this directory
		File[] srcFiles = directory.listFiles() ;
		Credentials restCred= new Credentials(RestPropertiesUtil.getInstance().getProperty("userid"), 
				RestPropertiesUtil.getInstance().getProperty("password")) ;
		for(File aSrc: srcFiles) {
			FileReader reader = new FileReader(aSrc) ; 
			JSONObject json = jsonUtil.parseJson(reader) ;
			SourceInfo si = extractSourceInfo(json) ;
			MetadataInfo mi = getMetaInfo() ;
			TargetInfo ti = extractTargetInfo(json, mi.getHostName()) ;
			
			String suff = new Date().toString().replaceAll(":","_").replaceAll(" ", "_") ;
			si.setName(si.getName() + "_" + suff);
			sourceService = new SourceService(mi, si, ti) ;
			String id = sourceService.insert(); 
			insertedIds.add(id) ;
			sourceService.getsInfo().setSourceId(id);
			
			logger.info("Doing metadata crawl");
			
			
			sourceService.metaDataCrawl(restCred);
			
			List<String> tableids = sourceService.getListOfTablesInSource(restCred) ;
			TableGroupInfo tgInfo = new TableGroupInfo() ;
			tgInfo.setTableIds(tableids);
			logger.info("creating table group for all tables");
			String tableGrpId = sourceService.createTGWithTableIds(restCred,
					"basicTableGroup.tpl",
					tgInfo
					)
			;
			sourceService.dataCrawl(restCred, tableGrpId);
			sourceService.deleteSource(new BasicDBObject("_id", id), ti);
		}
	}
	
	
	private MetadataInfo getMetaInfo() {
		MetadataInfo mi = new MetadataInfo() ;
		mi.setCollectionName("sources");
		mi.setDbName(pu.getProperty("dbname"));
		mi.setHostName(pu.getProperty("hostname"));
		mi.setUserId(pu.getProperty("userid"));
		mi.setPassword(pu.getProperty("password"));
		mi.setPort(27017);
		return mi ;
	}
	
	private SourceInfo extractSourceInfo(JSONObject json) { 
		SourceInfo si =null;
		String srcType = json.get("sourceType").toString();
		logger.info("src type is" + srcType);
		if(isRDBMS(srcType)) {
			si = new RDBMSSouceInfo();
			JSONObject conn =(JSONObject) json.get("connection") ;
			si.setHostName(conn.get("host").toString());
			((RDBMSSouceInfo)si).setPort(new Integer(conn.get("port").toString()));
			((RDBMSSouceInfo)si).setSchema(conn.get("schema").toString());
			((RDBMSSouceInfo)si).setSourceDriverName(SourceDriverName.ORACLE_DRIVER_NAME);
			si.setCredentials(new Credentials(conn.get("userid").toString() , conn.get("password").toString()));
			si.setDbName(json.get("dbname").toString());
		}
		else {
			si = new SourceInfo() ;
		}
		si.setStype(srcType);
		si.setName(json.get("name").toString());
		
		return si;
		
	}
	
	private boolean isRDBMS(String srcType) {
		return "oracle".equalsIgnoreCase(srcType) 
				|| "teradata".equalsIgnoreCase(srcType)
				|| "sqlserver".equalsIgnoreCase(srcType);	
			
	}
	
	private TargetInfo extractTargetInfo(JSONObject json, String hdfsHost) { 
		TargetInfo ti = new TargetInfo() ;
		ti.setHdfsHost(hdfsHost);
		ti.setHdfsPath(json.get("hdfs_path").toString());
		ti.setHiveSchema(json.get("hive_schema").toString());
		ti.setHdfsPort(new Integer(pu.getProperty("hdfsport")));
		ti.setHivePort(new Integer(pu.getProperty("hiveport")));
		return ti ;
	}
	
	public static void main(String[] args) throws Exception {
		IngestionHelper service = new IngestionHelper() ; 
		service.parseFiles(); 
	}
}
