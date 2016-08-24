package io.infoworks.util;

import java.sql.DriverManager;
import java.util.logging.Logger;
import java.sql.Connection;

public class HiveUtil {

	private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver" ; 
	private String hiveHost ;
	private int hivePort ;
	private String dbName ;
	private String hiveConfigurationVariables ;
	private Connection conn ; 
	
	private Logger logger = Logger.getLogger("HiveUtil") ;
	
	public HiveUtil(String hiveHost, int hivePort, String dbName, String hiveConfigurationVariables) { 
		this.hiveHost = hiveHost ;
		this.dbName = dbName ;
		this.hivePort = hivePort ; 
		this.hiveConfigurationVariables = hiveConfigurationVariables ;
	}
	
	public void executeStatement(String sql) throws Exception { 
		try {
			Class.forName(HIVE_DRIVER_NAME) ; 
			if(conn == null) { 
				conn = DriverManager.getConnection(
						makeHiveJdbcString(dbName, hiveConfigurationVariables), "hive", "") ;
			}
			
			int rowsImpacted = conn.createStatement().executeUpdate(sql) ;
			logger.info("rows impacted are " + rowsImpacted);
		}
		catch(Exception e) { 
			e.printStackTrace();
			logger.severe("ignoring this exception for time being");
		}
	}
	
	private String makeHiveJdbcString(String dbName, String hiveConfigurationVariables) { 
		StringBuilder sb = new StringBuilder() ; 
		sb.append("jdbc:hive://").append( hiveHost).append(":").append(hivePort).append("/").append( dbName);
		
		if(hiveConfigurationVariables != null) { 
			sb.append("?").append(hiveConfigurationVariables) ;
		}
		
		return sb.toString() ;
	}
	
	
}
