package io.infoworks.ingestion.source;

public class RDBMSSouceInfo extends SourceInfo {
	
	private int port ;
	private String schema ;
	private SourceDriverName sourceDriverName ;
	
	private transient String jdbcUrl ;
	

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
	
	public SourceDriverName getSourceDriverName() {
		return sourceDriverName;
	}
	public void setSourceDriverName(SourceDriverName sourceDriverName) {
		this.sourceDriverName = sourceDriverName;
	}
	@Override
	public String toString() {
		return "RDBMSSouceInfo [port=" + port + ", schema=" + schema + ", sourceDriverName=" + sourceDriverName
				+ ", toString()=" + super.toString() + "]";
	}

	
}
