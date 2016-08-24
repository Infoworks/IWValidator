package io.infoworks.util;

import java.util.logging.Logger;

import io.infoworks.ingestion.metadata.Credentials;

public class RestUtil {

	private static RestUtil restUtil = new RestUtil(); 
	private Logger logger = Logger.getLogger("RestUtil") ;
	
	private RestUtil() { 
		
	}
	
	public static RestUtil getInstance() { 
		return restUtil ;
	}
	
	public String getMetaCrawlUrl(String hostName, String sourceId, Credentials cred) {
		return "http://" + hostName + ":2999/v1.1/source/crawl_metadata.json?source_id=" + sourceId + "&user="
				+ cred.getUserId() + "&pass=" + cred.getPassword();
	}
	
	public String getCreateTGUrl(String hostName, String sourceId, Credentials cred) {
		return  "http://" + hostName + ":2999" +
		"/v1.1/source/table_group/create.json?user=" + cred.getUserId() + "&pass=" + cred.getPassword()  + "&source_id=" + sourceId ;
	}
	
	public String getListTablesInSourceUrl(String hostName, String sourceId , Credentials cred) { 
		return new StringBuilder().append("http://").append(hostName).append(":2999").append("/v1.1/source/tables.json")
			.append("?")
			.append("user=")
			.append(cred.getUserId())
			.append("&")
			.append("pass=")
			.append(cred.getPassword())
			.append("&")
			.append("source_id=")
			.append(sourceId).toString();
	}
	
	
	//GET http://54.172.95.202:2999/v1.1/job/status.json?job_id=9f5846008dd136f2bb9b7e07&auth_token=aW5mb3dvcmtzOmFkbWluQGluZm93b3Jrcy5pbzoxMjM0NTY%3D
	public String getJobPollUrl(String hostName, String jobId, Credentials cred) { 
		return new StringBuilder().append("http://")
				.append(hostName)
				.append(":2999")
				.append("/v1.1/job/status.json?job_id=")
				.append(jobId)
				.append("&")
				.append("user=")
				.append(cred.getUserId())
				.append("&")
				.append("pass=")
				.append(cred.getPassword())
				.toString() ;
	}
	
	
	
	// POST http://54.172.95.202:2999/v1.1/source/table_group/ingest.json?table_group_id=f8fae678ecc55af56f737960&ingestion_type=source_all&auth_token=aW5mb3dvcmtzOmFkbWluQGluZm93b3Jrcy5pbzoxMjM0NTY%3D
	public String getDataCrawlUrl(String hostName, Credentials cred, String tgId) {
		return new StringBuilder().append(getRestBaseUrl(hostName))
				.append("source/table_group/ingest.json?table_group_id=")
				.append(tgId)
				.append("&ingestion_type=source_all&")
				.append("")
				.append(getAuthContext(cred))
				.toString() ;
	}
	
	
	// GET http://54.172.95.202:2999/v1.1/source/table_groups.json?source_id=4b6a2f56316bc45b7db52bda&auth_token=aW5mb3dvcmtzOmFkbWluQGluZm93b3Jrcy5pbzoxMjM0NTY%3D  

	public String getListTableGroupsForSourceUrl(String hostName, Credentials cred, String srcId) { 
		return new StringBuilder().append(getRestBaseUrl(hostName))
				.append("source/table_groups.json?source_id=")
				.append(srcId)
				.append("&")
				.append(getAuthContext(cred))
				.toString() ;
	}
	
	
	private String getAuthContext(Credentials cred) { 
		return new StringBuilder().append("user=")
		.append(cred.getUserId())
		.append("&")
		.append("pass=")
		.append(cred.getPassword()).toString() ;
	}
	
	private String getRestBaseUrl(String hostName) { 
		return new StringBuilder().append("http://")
				.append(hostName)
				.append(":2999")
				.append("/v1.1/").toString() ;
	}
	
	
}
