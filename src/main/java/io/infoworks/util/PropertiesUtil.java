package io.infoworks.util;

import java.util.Properties;
import java.util.logging.Logger;

public class PropertiesUtil {

	private static PropertiesUtil pu = 	new  PropertiesUtil();
	private Logger logger 			=	Logger.getLogger("PropertiesUtil") ;
	private Properties props 		= new Properties() ; 
	
	private PropertiesUtil()  {
		try {
			init() ;
		}
		catch(Exception e) { 
			e.printStackTrace();
			logger.severe("error occurred." + e.getMessage());
		}
	}
	
	public static PropertiesUtil getInstance() { 
		return pu;
	}
	
	private void init() throws Exception {
		
		props.load(getClass().getResourceAsStream("metainfo.properties"));
	}
	
	public String getProperty(String key) {
		return props.getProperty(key) ;
	}
}
