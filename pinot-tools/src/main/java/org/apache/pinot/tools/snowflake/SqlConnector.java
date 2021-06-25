/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools.snowflake;

import groovy.sql.Sql;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.hadoop.fs.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SqlConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlConnector.class);

  private SqlConnectorConfig _sqlConnectorConfig;
  private String _pinotTable;

  protected void setSqlConnectorConfig(SqlConnectorConfig sqlConnectorConfig) {
    _sqlConnectorConfig = sqlConnectorConfig;
  }

  protected void setPinotTable(String pinotTable) {
    _pinotTable = pinotTable;
  }

  protected Statement getJDBCConnection(SqlConnectorConfig sqlConnectorConfig) throws SQLException {
    verifyDriverPresent();

    LOGGER.info("Creating JDBC connection");
    Connection connection =  DriverManager.getConnection(
        sqlConnectorConfig.getConnectString(),
        sqlConnectorConfig.getConnectProperties()
    );

    LOGGER.info("Done creating JDBC connection");
    return connection.createStatement();
  }

  public void execute() throws Exception {
    if (_sqlConnectorConfig == null) {
      throw new IllegalStateException("SqlConnectorConfig not set");
    }
    // Get JDBC connection
    Statement statement = getJDBCConnection(_sqlConnectorConfig);

    // Read data in chunks
    batchReadData(statement);

    // Generate segments and upload
    buildAndPushSegments();

    statement.close();
  }

  abstract void verifyDriverPresent() throws IllegalStateException;

  abstract void batchReadData(Statement statement) throws Exception; //TODO make Exception more specific

  abstract void buildAndPushSegments();
}
