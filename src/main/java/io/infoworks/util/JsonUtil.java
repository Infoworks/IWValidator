package io.infoworks.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

	private Logger logger = Logger.getLogger("JsonUtil") ;
	
	public JSONObject parseJson(FileReader reader) throws Exception {
		JSONParser jsonParser = new JSONParser();
		return (JSONObject) jsonParser.parse(reader);
	}

	
	 public  <T> T parse(String input, Class<T> type) throws IOException {
	        ObjectMapper mapper = new ObjectMapper();
	        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	        return mapper.readValue(input, type);
	    }
	
	public static void main(String[] args) throws Exception {
		JsonUtil ju = new JsonUtil();
		
		System.out.println(ju.parse("{\"error\":\"null\",\"result\":\"9493390607e70fcd04856d1d\"}                                                                                                                                                                                               ", HashMap.class)) ;
	}
}
