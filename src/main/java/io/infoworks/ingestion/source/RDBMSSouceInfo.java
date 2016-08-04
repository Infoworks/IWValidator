package io.infoworks.ingestion.source;

public class RDBMSSouceInfo extends SourceInfo {
	
	private String hostname ;
	private int port ;
	private String schema ;
	private String userName ;
	private String password ;
	private SourceDriverName sourceDriverName ;
	
	private transient String jdbcUrl ;
	
	public RDBMSSouceInfo(String hostname, int port, String schema, 
							String userName, 
							String password, 
							SourceDriverName sourceDriverName) {
		this.hostname = hostname ;
		this.port = port ;
		this.schema = schema ;
		this.userName = userName ;
		this.password = password ;
		this.sourceDriverName = sourceDriverName ;
		
		
	}
	
	public String getHostname() {
		return hostname;
	}
	public void setHostname(String hostname) {
		this.hostname = hostname;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getJdbcUrl() {
		return jdbcUrl;
	}
	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public SourceDriverName getSourceDriverName() {
		return sourceDriverName;
	}
	public void setSourceDriverName(SourceDriverName sourceDriverName) {
		this.sourceDriverName = sourceDriverName;
	}
	
}
