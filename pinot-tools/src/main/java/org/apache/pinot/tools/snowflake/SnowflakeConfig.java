package org.apache.pinot.tools.snowflake;

import java.util.Properties;


/**
 * Snowflake related configuration
 */
public class SnowflakeConfig implements SqlConnectorConfig {

  //Snowflake user name
  public String _username;

  //Snowflake password
  public String _password;

  //Snowflake account name
  public String _account;

  //Snowflake database
  public String _database;

  //Snowflake schema
  public String _schema;

  //Snowflake table
  public String _table;

  public SnowflakeConfig(String username, String password, String account, String database, String schema, String table) {
    _username = username;
    _password = password;
    _account = account;
    _database = database;
    _schema = schema;
    _table = table;
  }

  public String getTable() {
    return _table;
  }

  @Override
  public Properties getConnectProperties() {
    Properties properties = new Properties();
    properties.put("user", _username);     // TIMSANTS
    properties.put("password", _password); // "egh9SMUD!thuc*toom"
    properties.put("account", _account);  // "xg65443"
    properties.put("db", _database);       // "SNOWFLAKE_SAMPLE_DATA"
    properties.put("schema", _schema);   // "TPCH_SF001"
    return properties;
  }

  @Override
  public String getConnectString() {
    // create a new connection
    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");

    // use the default connection string if it is not set in environment
    if (connectStr == null) {
      connectStr = "jdbc:snowflake://" + _account + ".snowflakecomputing.com"; // replace accountName with your account name
    }

    return connectStr;
  }
}
