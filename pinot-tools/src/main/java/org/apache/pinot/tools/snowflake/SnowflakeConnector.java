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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SQL connector implementation for importing data from Snowflake
 */
public class SnowflakeConnector extends SqlConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnector.class);

  public SnowflakeConnector(SqlConnectorConfig sqlConnectorConfig, SqlQueryConfig sqlQueryConfig,
      String pinotControllerUrl, String pinotTable) {
    setSqlConnectorConfig(sqlConnectorConfig);
    setSqlQueryConfig(sqlQueryConfig);
    setPinotControllerUrl(pinotControllerUrl);
    setPinotTable(pinotTable);
  }

  @Override
  void verifyDriverPresent() throws IllegalStateException {
    try {
      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Snowflake driver not found", e);
    }
  }
}
