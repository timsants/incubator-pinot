package org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SnowflakeConnectorPlugin {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectorPlugin.class);

  public static Statement getJDBCConnection(String username, String password, String account, String database, String schema) throws SQLException {
    verifyDriverPresent();

    LOGGER.info("Creating JDBC connection");

    Properties properties = new Properties();
    properties.put("user", username);
    properties.put("password", password);
    properties.put("account", account);
    properties.put("db", database);
    properties.put("schema", schema);
    Connection connection =  DriverManager.getConnection(getConnectString(account), properties);

    LOGGER.info("Done creating JDBC connection");
    return connection.createStatement();
  }

  public static String getConnectString(String account) {
    // create a new connection
    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");

    // use the default connection string if it is not set in environment
    if (connectStr == null) {
      connectStr = "jdbc:snowflake://" + account + ".snowflakecomputing.com"; // replace accountName with your account name
    }

    return connectStr;
  }

  public static void verifyDriverPresent() throws IllegalStateException {
    try {
      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Snowflake driver not found", e);
    }
  }
}
