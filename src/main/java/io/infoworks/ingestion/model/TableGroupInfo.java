package io.infoworks.ingestion.model;

import java.util.List;

public class TableGroupInfo {

	private List<String> tableIds ;
	private String scheduling;
	private String combinedSchedule ;
	private String cdcSchedule ;
	private String mergeSchedule; 
	private String fullLoadSchedule ;
	private String dataExpirySchedule ;
	
	public List<String> getTableIds() {
		return tableIds;
	}
	public void setTableIds(List<String> tableIds) {
		this.tableIds = tableIds;
	}
	public String getScheduling() {
		return scheduling;
	}
	public void setScheduling(String scheduling) {
		this.scheduling = scheduling;
	}
	public String getCombinedSchedule() {
		return combinedSchedule;
	}
	public void setCombinedSchedule(String combinedSchedule) {
		this.combinedSchedule = combinedSchedule;
	}
	public String getCdcSchedule() {
		return cdcSchedule;
	}
	public void setCdcSchedule(String cdcSchedule) {
		this.cdcSchedule = cdcSchedule;
	}
	public String getMergeSchedule() {
		return mergeSchedule;
	}
	public void setMergeSchedule(String mergeSchedule) {
		this.mergeSchedule = mergeSchedule;
	}
	public String getFullLoadSchedule() {
		return fullLoadSchedule;
	}
	public void setFullLoadSchedule(String fullLoadSchedule) {
		this.fullLoadSchedule = fullLoadSchedule;
	}
	public String getDataExpirySchedule() {
		return dataExpirySchedule;
	}
	public void setDataExpirySchedule(String dataExpirySchedule) {
		this.dataExpirySchedule = dataExpirySchedule;
	}
	
}
