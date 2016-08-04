package io.infoworks.ingestion.source;

public enum SourceDriverName {
	
	ORACLE_DRIVER_NAME("oracle.jdbc.driver.OracleDriver") ;
	
	private final String driverName ;
	
	SourceDriverName(final String  driverName) { 
		this.driverName = driverName ;
	}
	
	@Override
	public String toString() { 
		return driverName ;
	}
}
