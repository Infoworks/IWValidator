package io.infoworks.cube;
/**
 * Created by abhishek on 06/08/16.
 */

import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class QueryExecutor {

  private static final Logger logger = Logger.getLogger(QueryExecutor.class.getName());

  private static String kylinHost;
  private static String kylinPort;
  private static String kylinUsername;
  private static String kylinPassword;
  private static String kylinProjectName;

  private final String kylingJDBCDriver = "org.apache.kylin.jdbc.Driver";

  public QueryExecutor(String kylinHost, String kylinPort, String kylinUsername, String kylinPassword, String kylinProjectName) {
    this.kylinHost = kylinHost;
    this.kylinPort = kylinPort;
    this.kylinUsername = kylinUsername;
    this.kylinPassword  = kylinPassword;
    this.kylinProjectName = kylinProjectName;
  }

  private static String getConnectionString() {
    return "jdbc:kylin://" + kylinHost + ":" + kylinPort + "/" + kylinProjectName;
  }

  private static Connection getConnection() throws Exception {
    Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
    Properties info = new Properties();
    info.put("user", kylinUsername);
    info.put("password", kylinPassword);
    Connection conn = driver.connect(getConnectionString(), info);
    return conn;
  }

  private static ResultSet executeQuery(String query) throws Exception {
    Preconditions.checkNotNull(query, "SQL query cannot be empty.");
    Connection conn = getConnection();
    Statement stat = conn.createStatement();
    ResultSet resultSet = stat.executeQuery(query);
    return resultSet;
  }

  private static File[] getFileList() {
    File folder = new File(System.getProperty("user.dir") + "/src/resources");
    File[] fileList = folder.listFiles();
    return fileList;
  }

  private static List<String> getQueryList() throws Exception {
    List<String> queryList= new ArrayList<String>();

    File[] files = getFileList();
    for (int i=0; i<files.length; i++) {
      if (files[i].isFile()) {
        BufferedReader bReader = null;
        try {
          File file = new File(files[i].getAbsolutePath());
          bReader = new BufferedReader(new FileReader(file));
          String queryLine;
          while ((queryLine = bReader.readLine()) != null) {
            queryList.add(queryLine);
          }
        }
        finally {
          if (bReader != null) {
            bReader.close();
          }
        }
      }
    }
    return queryList;
  }

  public static void executeQueriesFromFile() throws Exception {
    ResultSet resultSet = null;
    List<String> queryList = getQueryList();
    for (String query : queryList) {
      try {
        resultSet = executeQuery(query);
        // Print result set here
      }
      finally {
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }
}
