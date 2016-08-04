package io.infoworks.ingestion.source;

public enum SourceType {
	ORACLE("ORACLE"), MYSQL("MYSQL"), TERADATA("TERADATA");

	private final String stype;

	SourceType(final String stype) {
		this.stype = stype;
	}

	@Override
	public String toString() { 
		return this.stype ;
	}
}
