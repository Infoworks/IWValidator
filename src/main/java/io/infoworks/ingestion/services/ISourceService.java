package io.infoworks.ingestion.services;

import io.infoworks.ingestion.metadata.Credentials;
import io.infoworks.ingestion.model.TableGroupInfo;
import io.infoworks.validation.IValidator;
import io.infoworks.validation.ValidationResponse;

import java.util.List ; 

public interface ISourceService {

	public String insert() ;
	
	public void metaDataCrawl(Credentials restCred) throws Exception ;
	
	public String createTGWithTableIds(Credentials restCred, String tgTblName, TableGroupInfo tg) throws Exception ; 
	
	public void dataCrawl(Credentials restCred, String tableGroupId) throws Exception ;
	
	public List<ValidationResponse> validate(List<IValidator> validators) ;
}
