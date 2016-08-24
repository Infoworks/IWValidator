package io.infoworks.util;

import java.net.URI;
import java.util.logging.Logger;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter.DEFAULT;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {

	private static String DEFAULT_SCHEME = "hdfs" ; 
	private static Logger logger = Logger.getLogger("HdfsUtil") ;
	
	public static void deletePath(String host, String  port, String scheme, 
			String filePath) {
		try {
			if(scheme == null) {
				scheme = DEFAULT_SCHEME ;
			}
			String uri = createUri(scheme, host, port, filePath) ;
			Path path = createPath(uri) ;
			logger.info("Deleting hdfs path " + uri);
			FileSystem file = getFileSystem(uri, new Configuration()) ;
			try {
				boolean isDeleted = file.delete(path, true) ;
				logger.info("Is file " + path + " deleted." + isDeleted);
			}
			catch(Exception ex) { 
				ex.printStackTrace();
				logger.severe("ignoring the exception for time being");
			}
			
		}
		catch(Exception e) {
			e.printStackTrace();
			logger.severe("error occurred." + e.getMessage() );
			throw new RuntimeException(e) ;
		}
	}
	
	private static FileSystem getFileSystem(String uri, Configuration conf) throws Exception {
		return FileSystem.get (new URI(uri), conf); 
	}
	
	private static Path createPath(String uri) {
		return new Path(uri) ;
	}
	
	private static String createUri(String scheme, String hostname, 
			String port, String filepath) {
		
		return new StringBuilder(scheme)
			.append("://")
			.append(hostname)
			.append(":")
			.append("" + port)
			.append("/")
			.append(filepath).toString() ;
			
	}
}
