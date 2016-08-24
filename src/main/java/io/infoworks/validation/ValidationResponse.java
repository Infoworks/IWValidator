package io.infoworks.validation;

import java.util.ArrayList;
import java.util.List;

public class ValidationResponse {

	private boolean hasError = false ; 
	private List<String> errorMessages = new ArrayList<String>() ;
	public boolean isHasError() {
		return hasError;
	}
	public void setHasError(boolean hasError) {
		this.hasError = hasError;
	}
	public List<String> getErrorMessages() {
		return errorMessages;
	}
	
	public void addErrorMessage(String errorMessage) {
		errorMessages.add(errorMessage);
	} 
	
}
