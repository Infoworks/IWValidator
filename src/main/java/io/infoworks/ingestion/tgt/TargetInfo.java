package io.infoworks.ingestion.tgt;

public class TargetInfo {

	private String hdfsPath;
	private String hiveSchema ;
	
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
	
}
