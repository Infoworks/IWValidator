package io.infoworks.ingestion.tgt;

public class TargetInfo {

	private String hdfsHost = "localhost" ;
	private String hdfsPath;
	private String hiveSchema ;
	private int hdfsPort ;
	private int hivePort ;
	
	public String getHdfsPath() {
		return hdfsPath;
	}
	public void setHdfsPath(String hdfsPath) {
		this.hdfsPath = hdfsPath;
	}
	public String getHiveSchema() {
		return hiveSchema;
	}
	public void setHiveSchema(String hiveSchema) {
		this.hiveSchema = hiveSchema; 
	}
	
	public int getHdfsPort() {
		return hdfsPort;
	}
	public void setHdfsPort(int hdfsPort) {
		this.hdfsPort = hdfsPort;
	}
	public String getHdfsHost() {
		return hdfsHost;
	}
	public void setHdfsHost(String hdfsHost) {
		this.hdfsHost = hdfsHost;
	}
	@Override
	public String toString() {
		return "TargetInfo [hdfsHost=" + hdfsHost + ", hdfsPath=" + hdfsPath + ", hiveSchema=" + hiveSchema
				+ ", hdfsPort=" + hdfsPort + "]";
	}
	public int getHivePort() {
		return hivePort;
	}
	public void setHivePort(int hivePort) {
		this.hivePort = hivePort;
	}
	
	
	
}
