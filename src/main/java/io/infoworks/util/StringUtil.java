package io.infoworks.util;

import org.apache.commons.codec.binary.Base64;

public class StringUtil {

	public static String encodeBase64(String str) {
		return new String(Base64.encodeBase64(str.getBytes())) ;
	}
}
