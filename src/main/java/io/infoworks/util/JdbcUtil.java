package io.infoworks.util;

public class JdbcUtil {
	

	public String createOracleJdbcUrl(String hostName, int port,  String dbName) { 
		return new StringBuilder().append("jdbc").append(":").append("oracle").
			append(":").append("thin").append(":").append("@").append(hostName)
			.append(":")
			.append(""+ port)
			.append(":")
			.append(dbName).toString() ;
	}

}
