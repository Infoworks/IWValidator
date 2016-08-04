package io.infoworks.ingestion;

import java.util.logging.Logger;

import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;

import io.infoworks.ingestion.metadata.Credentials;

public class BaseRestServices {

	private Logger logger = Logger.getLogger("BaseRestServices") ;
	
	protected void rdbmsMetaCrawl(String hostName, String sourceId, Credentials cred) throws Exception { 
		HttpClient http = createClient() ;
		
		
		HttpPost post = new HttpPost(getMetaCrawlReq(hostName, sourceId, cred));
		logger.info("url for post " + getMetaCrawlReq(hostName, sourceId, cred));
		//StringEntity entity = new StringEntity(getMetaCrawlReq(hostName, sourceId, cred)) ;
		//post.setEntity(entity);
		
		logger.info("starting meta crawl");
		HttpResponse resp = http.execute(post) ;
		logger.info("done meta crawl, validating now");
		validateResponse(resp) ;
		
	}
	
	protected String getMetaCrawlReq(String hostName, String sourceId, Credentials cred) {
		return "http://" +hostName +  ":2999/v1.1/source/crawl_metadata.json?source_id="+ sourceId + "&user=" + cred.getUserId() + "&pass=" + cred.getPassword() ;
	}
	
	public HttpClient createClient() {
		return HttpClientBuilder.create().build()  ;
	}
	
	public void validateResponse(HttpResponse resp) {
		StatusLine sl = resp.getStatusLine() ;
		logger.info("Status Code:" + sl.getStatusCode());
		logger.info("Reason : " + sl.getReasonPhrase());
		
		if(sl.getStatusCode() > 202) { 
			throw new RuntimeException("Resp code is not 200 " + sl.getStatusCode() 
			+ "," + sl.getReasonPhrase()) ;
		}
	}
	
	public static void main(String[] args) throws Exception  {
		BaseRestServices bSvc = new BaseRestServices() ;
		bSvc.rdbmsMetaCrawl("52.91.51.9","57a2275c78353a6cde87b68b", new Credentials("admin@infoworks.io", "123456"));
		
	}
}
