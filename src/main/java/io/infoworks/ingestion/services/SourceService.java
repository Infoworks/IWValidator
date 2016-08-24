package io.infoworks.ingestion.services;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import io.infoworks.ingestion.metadata.Credentials;
import io.infoworks.ingestion.metadata.MetadataInfo;
import io.infoworks.ingestion.model.TableGroupInfo;
import io.infoworks.ingestion.source.RDBMSSouceInfo;
import io.infoworks.ingestion.source.SourceDriverName;
import io.infoworks.ingestion.source.SourceInfo;
import io.infoworks.ingestion.source.SourceType;
import io.infoworks.ingestion.tgt.TargetInfo;
import io.infoworks.util.HdfsUtil;
import io.infoworks.util.HiveUtil;
import io.infoworks.util.HttpUtil;
import io.infoworks.util.JdbcUtil;
import io.infoworks.util.JsonUtil;
import io.infoworks.util.MongoUtil;
import io.infoworks.util.RestUtil;
import io.infoworks.util.StringUtil;
import io.infoworks.util.TemplateUtil;
import io.infoworks.validation.IValidator;
import io.infoworks.validation.ValidationResponse;

import static com.google.common.base.Preconditions.*;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.spec.RSAOtherPrimeInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.time.StopWatch;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

public class SourceService implements ISourceService {

	// Constants
	private static final int POLL_TIMEOUT_MILLI_SECS  = 7200000 ; // 2 hrs
	private Logger logger = Logger.getLogger("SourceService");
	private MongoUtil mongoUtil = null;
	private JdbcUtil jdbcUtil = null;
	private HttpUtil httpUtil = null;
	private HiveUtil hiveUtil = null;
	private JsonUtil jsonUtil = null; 
	private RestUtil restUtil = null;
	private TemplateUtil templateUtil = null;
	private MetadataInfo mInfo;
	private SourceInfo sInfo;
	private TargetInfo tInfo;

	public SourceService(MetadataInfo minfo, SourceInfo sInfo, TargetInfo tInfo) {
		jdbcUtil = new JdbcUtil();
		httpUtil = new HttpUtil();
		mongoUtil = new MongoUtil(minfo);
		templateUtil = new TemplateUtil();
		jsonUtil = new JsonUtil() ;
		restUtil = RestUtil.getInstance();
		
		hiveUtil = new HiveUtil(tInfo.getHdfsHost(), tInfo.getHivePort(), tInfo.getHiveSchema(), null) ;
		this.mInfo = minfo;
		this.sInfo = sInfo;
		this.tInfo = tInfo;
	}

	public String insert() {
		logger.info("rdbms source found: " + sInfo.getName());
		RDBMSSouceInfo rsInfo = (RDBMSSouceInfo) sInfo;
		if ("oracle".equalsIgnoreCase(rsInfo.getStype())) {
			logger.info("oracle src found");
			String jdbcUrl = jdbcUtil.createOracleJdbcUrl(rsInfo.getHostName(), rsInfo.getPort(), rsInfo.getDbName() );
			rsInfo.setJdbcUrl(jdbcUrl);
			BasicDBObject src = mongoUtil.constructSourceDBObj(rsInfo, tInfo);
			DBCollection collection = mongoUtil.getCollection(mInfo.getCollectionName());
			WriteResult result = collection.save(src);
			logger.info("upserted id is " + result.getUpsertedId() + "," + result);
			return mongoUtil.findSource(sInfo.getName()).get("_id").toString();
			// return result.getUpsertedId() ;
		}

		return null;

	}

	public void metaDataCrawl( Credentials restCred) throws Exception {

		HttpClient http = httpUtil.createClient();
		HttpPost post = new HttpPost(RestUtil.getInstance().getMetaCrawlUrl(mInfo.getHostName(), sInfo.getSourceId(), restCred));

		logger.info("starting meta crawl @ " + RestUtil.getInstance().getMetaCrawlUrl(mInfo.getHostName(), sInfo.getSourceId(), restCred));
		HttpResponse resp = http.execute(post);
		logger.info("submitted meta crawl req, validating now");
		httpUtil.validateResponse(resp);
		//Map<String, Object> mapResp = jsonUtil.parse(httpUtil.getStringResponse(resp), HashMap.class);
		//String jobId = (String) mapResp.get("result") ;
		String jobId = httpUtil.getJobId(resp);
		logger.info("Got job id , polling now " + jobId) ;
		String jobStatusUrl = restUtil.getJobPollUrl(mInfo.getHostName(), jobId, restCred) ;
		String status = httpUtil.pollForJob(jobStatusUrl, 300000, 3000); /// 3 milliseconds gap between each poll
		logger.info("metadata crawl is over");
		

	}
	

	
	@SuppressWarnings("unchecked")
	public List<String> getListOfTablesInSource(Credentials restCred) throws Exception { 
		String url = RestUtil.getInstance().getListTablesInSourceUrl(mInfo.getHostName(), sInfo.getSourceId(), restCred);
		logger.info("using url: " + url);
		HttpClient http = httpUtil.createClient();
		HttpGet get = new HttpGet(url) ;
		HttpResponse resp = http.execute(get) ;
		
		String strResp = httpUtil.getStringResponse(resp) ;
		
		logger.info("tables in source are : " + strResp);
		Map<String, Object> tables = jsonUtil.parse(strResp.toString(), HashMap.class) ;
		List<Map<String, String>> tablesList = (List<Map<String, String>>) tables.get("result") ;
		List<String> tableIds = new ArrayList<String>() ;
		
		for(Map<String, String> aTableInfo: tablesList) {
			tableIds.add(aTableInfo.get("id")) ;
		}
		logger.info("total number of tables in the tables List are :" + tableIds.size() );
		return tableIds ;
	}
		
	@SuppressWarnings("unchecked")
	public String createTGWithTableIds(Credentials restCred, String tgTblName, TableGroupInfo tg) throws Exception {
		
		String tgBody = templateUtil.getTableGrpFromTemplate(tgTblName, tg);
		logger.info("starting to create tablegroup, url " + RestUtil.getInstance().getCreateTGUrl(mInfo.getHostName(), sInfo.getSourceId(), restCred));
		
		HttpResponse resp = httpUtil.post(
				RestUtil.getInstance().getCreateTGUrl(mInfo.getHostName(), sInfo.getSourceId(), restCred), 
				tgBody) ;
		httpUtil.validateResponse(resp);	
		Map<String, Object> mapResp = jsonUtil.parse(httpUtil.getStringResponse(resp), HashMap.class) ;
		
		logger.info("submitted create tablegroup req, validating now. " + mapResp.get("result"));
		
		if(mapResp.get("error") != null && ! mapResp.get("error").toString().isEmpty()) { 
			throw new RuntimeException("Error occurred during table group create," + mapResp.get("error").toString()) ;
		}
		//TODO - Remove this workaround after Arpan is done with adding tgid in above response
		logger.info("getting tgs for src, " + restUtil.getListTableGroupsForSourceUrl(mInfo.getHostName(), restCred, sInfo.getSourceId()));
		HttpResponse getTGsResp = httpUtil.get(restUtil.getListTableGroupsForSourceUrl(mInfo.getHostName(), restCred, sInfo.getSourceId())) ;
		Map<String, Object> tgIdsMap = jsonUtil.parse(httpUtil.getStringResponse(getTGsResp), HashMap.class) ;
		
		List<Map<String, String>> tablesGrpsList = (List<Map<String, String>>) tgIdsMap.get("result") ;
		logger.info("tgids map" + tablesGrpsList.get(0).get("id"));
		
		return tablesGrpsList.get(0).get("id") ;
		
	}
	
	@Override
	public void dataCrawl(Credentials restCred, String tableGroupId) throws Exception {
		String  url = restUtil.getDataCrawlUrl(mInfo.getHostName(), restCred, tableGroupId);
		logger.info("data crawl url is " + url);
		HttpResponse resp = httpUtil.post(url, null) ;
		
		Map<String, Object> mapResp = jsonUtil.parse(httpUtil.getStringResponse(resp), HashMap.class) ;
		
		logger.info("submitted data crawl, validating now. " + mapResp.get("result"));
		httpUtil.validateResponse(resp);
		
		if(mapResp.get("error") != null && ! mapResp.get("error").toString().isEmpty()) { 
			throw new RuntimeException("Error occurred during data crawl," + mapResp.get("error").toString()) ;
		}		
		String jobId =mapResp.get("result").toString().replaceAll("\\[", "").replaceAll("\\]", "") ;
		logger.info("polling for job " + jobId);
		
		StopWatch sw = new StopWatch() ;
		sw.start();
		String status = httpUtil.pollForJob(restUtil.getJobPollUrl(mInfo.getHostName(), jobId, restCred), POLL_TIMEOUT_MILLI_SECS, 10000) ; // wait till 10 secs for polling
		sw.stop(); 
		logger.info("data crawl done with status " + status + ". It took time (milliseconds): " + sw.getTime());
		
	}

	public void deleteSource(DBObject id, TargetInfo ti) throws Exception {

		// logger.info("object deleted:" + getCollection("sources").remove(id)
		// .getN());
		// TODO - DELETE HDFS
		HdfsUtil.deletePath(ti.getHdfsHost(), "" + ti.getHdfsPort(), null, ti.getHdfsPath());
		// TODO - Drop Hive schema
		logger.info("dropping hive DB " + ti.getHiveSchema());
		hiveUtil.executeStatement("drop database " + ti.getHiveSchema());

	}

	public SourceInfo getsInfo() {
		return sInfo;
	}

	@Override
	public List<ValidationResponse> validate(List<IValidator> validators) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
