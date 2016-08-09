package io.infoworks.cube;
/**
 * Created by abhishek on 06/08/16.
 */

import com.google.common.base.Preconditions;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.sql.Driver;
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
  private static String queryFileName;

  private static final String kylingJDBCDriver = "org.apache.kylin.jdbc.Driver";

  public QueryExecutor(String kylinHost, String kylinPort, String kylinUsername, String kylinPassword, String kylinProjectName, String queryFileName) {
    this.kylinHost = kylinHost;
    this.kylinPort = kylinPort;
    this.kylinUsername = kylinUsername;
    this.kylinPassword  = kylinPassword;
    this.kylinProjectName = kylinProjectName;
    this.queryFileName = queryFileName;
  }

  private static String getConnectionString() {
    return new StringBuilder().append("jdbc:kylin://").append(kylinHost).append(":").append(kylinPort).append("/").append(kylinProjectName).toString();
  }

  private static Connection getConnection() throws Exception {
    Driver driver = (Driver) Class.forName(kylingJDBCDriver).newInstance();
    Properties info = new Properties();
    info.put("user", kylinUsername);
    info.put("password", kylinPassword);
    Connection conn = driver.connect(getConnectionString(), info);
    return conn;
  }

  private static ResultSet executeQuery(String query) throws Exception {
    Preconditions.checkNotNull(query, "SQL query cannot be empty.");
    logger.info("Trying to connect to KYLIN server...");
    long startTime, endTime;
    startTime = System.currentTimeMillis();
    Connection conn = getConnection();
    endTime = System.currentTimeMillis();
    logger.info("Connected to KYLIN server...");
    logger.info("Time elapsed in establishing connection: " + (float)(endTime-startTime)/1000 + " second(s)");
    Statement stat = conn.createStatement();
    logger.info("Executing SQL...");
    startTime = System.currentTimeMillis();
    ResultSet resultSet = stat.executeQuery(query);
    endTime = System.currentTimeMillis();
    logger.info("Time elapsed in executing SQL: " + (float)(endTime-startTime)/1000 + " second(s)");
    conn.close();
    return resultSet;
  }

  private static String getFolderPath() {
    File folder = new File(new StringBuilder().append(System.getProperty("user.dir")).append("/src/resources").toString());
    return folder.getAbsolutePath();
  }

  private static List<String> getQueryList() throws Exception {
    List<String> queryList= new ArrayList<String>();

    String filePath = getFolderPath() + "/" + queryFileName;
    BufferedReader bReader = null;
    try {
      File file = new File(filePath);
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
    return queryList;
  }

  public static void executeQueriesFromFile() throws Exception {
    ResultSet resultSet = null;
    List<String> queryList = getQueryList();
    for (String query : queryList) {
      try {
        logger.info("---------------------------------------------------------------");
        logger.info("Executing query: " + query);
        logger.info("---------------------------------------------------------------");
        resultSet = executeQuery(query);
        logger.info("---------------------------------------------------------------");
        logger.info("Result");
        logger.info("---------------------------------------------------------------");
        while (resultSet.next()) {
          ResultSetMetaData rsMetaData= resultSet.getMetaData();
          String resultRow = "";
          for (int i=1; i<rsMetaData.getColumnCount(); i++) {
            resultRow += resultSet.getString(i) + ", ";
          }
          System.out.println(resultRow);
        }
      }
      finally {
        if (resultSet != null) {
          resultSet.close();
        }
      }
    }
  }
}
