package io.infoworks.util;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Date;
import java.util.logging.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.tgt.TargetInfo;

public class MongoUtil {

	private Logger logger = Logger.getLogger("MongoUtil") ;
	private DB db ;
	private MetadataInfo mInfo ;
	
	public MongoUtil(MetadataInfo mInfo) { 
		this.mInfo = mInfo ;
		createClient() ;
	}
	
	public void createClient() {
		checkNotNull(mInfo.getHostName());
		MongoCredential credential = MongoCredential.createCredential(mInfo.getUserId(), mInfo.getDbName(), mInfo.getPassword().toCharArray());
		ServerAddress sa = new ServerAddress(mInfo.getHostName(), mInfo.getPort());
		MongoClient client = new MongoClient(sa, Arrays.asList(credential));
		this.db= client.getDB(mInfo.getDbName());

	}
	
	public BasicDBObject constructSourceDBObj(RDBMSSouceInfo rsInfo, TargetInfo tInfo) { 
		BasicDBObject dbObj = new BasicDBObject("name", rsInfo.getName() )
				.append("connection", 
						new BasicDBObject("connection_string", rsInfo.getJdbcUrl())
						.append("username", rsInfo.getCredentials().getUserId())
						.append("password", StringUtil.encodeBase64(rsInfo.getCredentials().getPassword()))
						.append("schema", rsInfo.getSchema().toUpperCase())
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
	
	public 	DBCollection getCollection( String collectionName	) { 
		return db.getCollection(collectionName) ;
	}
	

	public void deleteSource(DBObject id) {
		logger.info("object deleted:" + getCollection("sources").remove(id) .getN());
		
	}
	
	public DBObject findSource(String name) {
		BasicDBObject obj = new BasicDBObject() ;
		obj.append("name", name) ;
		return getCollection("sources").findOne(obj) ;
	}
	

}
