package io.infoworks.util;

import java.util.Properties;
import java.util.logging.Logger;

public class RestPropertiesUtil {

	private static RestPropertiesUtil pu = 	new  RestPropertiesUtil();
	private Logger logger 			=	Logger.getLogger("RestPropertiesUtil") ;
	private Properties props 		= new Properties() ; 
	
	private RestPropertiesUtil()  {
		try {
			init() ;
		}
		catch(Exception e) { 
			e.printStackTrace();
			logger.severe("error occurred." + e.getMessage());
		}
	}
	
	public static RestPropertiesUtil getInstance() { 
		return pu;
	}
	
	private void init() throws Exception {
		
		props.load(getClass().getResourceAsStream("restinfo.properties"));
	}
	
	public String getProperty(String key) {
		return props.getProperty(key) ;
	}
}
