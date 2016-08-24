package io.infoworks.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

public class HttpUtil {

	private Logger logger = Logger.getLogger("HttpUtil");

	public HttpClient createClient() {
		return HttpClientBuilder.create().build();
	}

	public HttpResponse post(String url, String payload) throws Exception {
		HttpClient http = createClient();
		HttpPost post = new HttpPost(url) ;
		if(payload != null) {
			StringEntity se = new StringEntity(payload) ;
			post.setEntity(se);
		}
		return http.execute(post) ;
	}
	
	public HttpResponse get(String url) throws Exception {
		HttpClient http = createClient();
		HttpGet post = new HttpGet(url) ;
		
		return http.execute(post) ;
	}
	
	public void validateResponse(HttpResponse resp) {
		StatusLine sl = resp.getStatusLine();
		logger.info("Status Code:" + sl.getStatusCode());
		logger.info("Reason : " + sl.getReasonPhrase());

		if (sl.getStatusCode() > 202) {
			throw new RuntimeException("Resp code is not 200 " + sl.getStatusCode() + "," + sl.getReasonPhrase());
		}
	}

	public String getStringResponse(HttpResponse resp) throws Exception {
		InputStream is = resp.getEntity().getContent();

		byte[] buffer = new byte[256] ; 
		StringBuilder bldr = new StringBuilder() ; 
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		int bytesRead;
		
		while(( bytesRead = is.read(buffer)) != -1) { 
			output.write(buffer, 0, bytesRead);
			
		}
		bldr.append(new String(output.toByteArray())) ;
		return bldr.toString() ;
	}
	
	public String pollForJob(String jobStatusUrl, int timeoutInMillis, int waitingAfterEachPoll) throws Exception { 
		long now = System.currentTimeMillis() ;
		HttpClient http = createClient();
		HttpGet get = new HttpGet(jobStatusUrl) ; 
		HttpResponse resp = null;
		
		while(true) { 
			resp = http.execute(get) ;
			validateResponse(resp);
			String strResp = getStringResponse(resp) ;
			JsonUtil jsonUtil = new JsonUtil() ;
			Map<String, Object> mapResp = jsonUtil.parse(strResp, HashMap.class) ;
			Map<String, Object> statusMap = (Map) mapResp.get("result") ;
			String status = (String) statusMap.get("status") ;
			if("failed".equalsIgnoreCase(status)) {
				throw new RuntimeException("the job has failed status") ;
			}
			
			if("completed".equalsIgnoreCase(status)) {
				logger.info("job is completed." );
				return status ;
			}
			
			logger.info("resp while polling is : " + mapResp);
			
			long timeelapsed = System.currentTimeMillis() - now ; 
			if(timeelapsed > timeelapsed) {
				logger.severe("timeput happened while polling for job : " + jobStatusUrl);
				return null;
			}
			if(waitingAfterEachPoll == 0) {
				Thread.sleep(3000);
			}
			else {
				Thread.sleep(waitingAfterEachPoll);
			}
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public String getJobId(HttpResponse resp) throws Exception { 
		JsonUtil jsonUtil = new JsonUtil() ;
		Map<String, Object> mapResp = jsonUtil.parse(getStringResponse(resp), HashMap.class);
		String jobId = (String) mapResp.get("result") ;
		
		return jobId; 
	}
}
