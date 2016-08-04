package io.infoworks.ingestion;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.source.SourceDriverName;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.source.SourceType;
import io.infoworks.ingestion.tgt.TargetInfo;
import io.infoworks.util.Util;

import static com.google.common.base.Preconditions.*;

import java.security.spec.RSAOtherPrimeInfo;
import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;

public class BaseMongoManager {

	private Logger logger = Logger.getLogger("BaseMongoManager") ;
	private DB db ;
	private MetadataInfo mInfo ;
	
	BaseMongoManager(MetadataInfo mInfo) {
		this.mInfo = mInfo ;
		db = getClient(mInfo.getHostName(), 	mInfo.getPort(), 
				  mInfo.getDbName(), 	mInfo.getUserId(), 
				  mInfo.getPassword()) ;		
	}
	
	protected DB getClient(String hostName, int port, String dbName, 
			String userName, String password) {
		checkNotNull(hostName);
		MongoCredential credential = MongoCredential.createCredential(userName, dbName, password.toCharArray());
		ServerAddress sa = new ServerAddress(hostName, port);
		MongoClient client = new MongoClient(sa, Arrays.asList(credential));
		return client.getDB(dbName);

	}

	protected String createOracleJdbcUrl(String hostName, int port, String schema) { 
		return new StringBuilder().append("jdbc").append(":").append("oracle").
			append(":").append("thin").append(":").append("@").append(hostName)
			.append(":")
			.append(""+ port)
			.append(":")
			.append(schema).toString() ;
	}
	

	
	public void insert(SourceInfo sInfo, TargetInfo tInfo, String suff) {
		if(sInfo instanceof RDBMSSouceInfo) { 
			
			RDBMSSouceInfo rsInfo = (RDBMSSouceInfo) sInfo ;
			if(rsInfo.getStype() == SourceType.ORACLE) { 
				String jdbcUrl = createOracleJdbcUrl(rsInfo.getHostname(), rsInfo.getPort(), rsInfo.getSchema()) ;
				rsInfo.setJdbcUrl(jdbcUrl);
				BasicDBObject src = constructMongoObj(rsInfo, tInfo, suff) ;
				DBCollection collection = getCollection(mInfo.getCollectionName()) ;
				WriteResult result = collection.save(src) ;
				logger.info("upserted id is "+ result.getUpsertedId() + "," + result);
			}
		}
		
	}
	
	protected DBCollection getCollection( String collectionName	) { 
		return db.getCollection(collectionName) ;
	}
	
	protected BasicDBObject constructMongoObj(RDBMSSouceInfo rsInfo, TargetInfo tInfo, String suff) { 
		BasicDBObject dbObj = new BasicDBObject("name", rsInfo.getName() )
				.append("connection", 
						new BasicDBObject("connection_string", rsInfo.getJdbcUrl())
						.append("username", rsInfo.getUserName())
						.append("password", Util.encodeBase64(rsInfo.getPassword()))
						.append("schema", rsInfo.getUserName().toUpperCase())
						.append("database", rsInfo.getStype().toString())
						.append("driver_name", rsInfo.getSourceDriverName().toString())
				 )
				.append("sourceType", "rdbms")
				.append("state", "ready")
				.append("createdBy", "automationTC" )
				.append("createdAt", new Date() ) 
				.append("hdfs_path", tInfo.getHdfsPath())
				.append("hive_schema", tInfo.getHiveSchema())
				;
		return dbObj ;
	}
	
	protected void deleteSource(DBObject id) {
		
		logger.info("object deleted:" + getCollection("sources").remove(id) .getN());
		//TODO - DELETE HDFS
		//TODO - Drop Hive schema
	}

	protected DBObject findSource(String name) {
		BasicDBObject obj = new BasicDBObject() ;
		obj.append("name", name) ;
		return getCollection("sources").findOne(obj) ;
	}
	
	
	public static void main(String[] args) {
		MetadataInfo mi = new MetadataInfo() ;
		//mi.setHostName("52.91.51.9"); 
		// mapr
		mi.setHostName("54.197.137.203"); 
		mi.setDbName("infoworks-new");
		mi.setUserId("infoworks");
		mi.setPassword("IN11**rk");
		mi.setCollectionName("sources");
		mi.setPort(27017);
		BaseMongoManager mgr = new BaseMongoManager(mi);
		
		
		RDBMSSouceInfo sInfo = new RDBMSSouceInfo("52.5.131.69", 1521, "xe","northwind", "IN11**rk", SourceDriverName.ORACLE_DRIVER_NAME) ;
		String suff = new Date().toString().replaceAll(":","_").replaceAll(" ", "_") ;
		String srcName = "autoname" +suff ;
		String hdfsPath = "/sources/automationsource_"  + suff ;
		String hiveSchema = "automationsource_"  + suff ;
		
		sInfo.setName(srcName);
		sInfo.setStype(SourceType.ORACLE);
		
		TargetInfo ti = new TargetInfo() ;
		
		ti.setHdfsPath(hdfsPath);
		ti.setHiveSchema(hiveSchema);
		
		mgr.insert(sInfo, ti, suff);
		
		mgr.deleteSource(mgr.findSource(srcName)) ;
	}

}
