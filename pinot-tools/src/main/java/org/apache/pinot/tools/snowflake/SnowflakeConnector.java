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

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.StartBrokerCommand;
import org.apache.pinot.tools.admin.command.StartControllerCommand;
import org.apache.pinot.tools.admin.command.StartServerCommand;
import org.apache.pinot.tools.admin.command.StartZookeeperCommand;
import org.apache.pinot.tools.admin.command.StopProcessCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;


public class SnowflakeConnector extends SqlConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnector.class);

  // Pinot table to import data into
  private String _pinotTable;
  private String _pinotControllerUrl;
  private String _pinotControllerPort;
  private String _pinotControllerHost;

  // Only used for testing. will not be part of connector.
  private File _testPinotTmpDir;
  private boolean _isStopped;


  public static void main(String args[]) throws Exception {
    SnowflakeConnector connector = new SnowflakeConnector();
    connector.initTestEnv();
    connector.init();
    connector.execute();
    //connector.stop();
  }

  private void stop() throws Exception {
    if (_isStopped) {
      return;
    }

    StopProcessCommand stopper = new StopProcessCommand(false);
    stopper.stopController().stopBroker().stopServer().stopZookeeper();
    stopper.execute();
    FileUtils.cleanDirectory(_testPinotTmpDir);

    _isStopped = true;
  }

  //TODO make into framework. snowflake impl.
  //parameters
  //sql format dialect

  private SnowflakeConnector initTestEnv() throws Exception {
    _pinotTable = "snowflakeTest";
    _pinotControllerHost = "pinot87d28eda.itpinot76696eaf.192.168.64.176.nip.io";
    _pinotControllerPort = "30000";
    _pinotControllerUrl = "http://" + _pinotControllerHost + ":" + _pinotControllerPort;

    //startCluster("mycluster");
    addTestTable(); //not needed in production.  only needed for testing.

    return this;
  }

  /**
   * Validate provided paramters.
   */
  private void init() throws IOException {
    // make sure _dataPullGranularity is not smaller than timeColumnFormatGranularity
    // make sure all template values are present

    SqlConnectorConfig sqlConnectorConfig = new SnowflakeConfig(
        "timsants",
        "egh9SMUD!thuc*toom",
        "xg65443.west-us-2.azure",
        "SNOWFLAKE_SAMPLE_DATA",
        "TPCH_SF1",
        "ORDERS"
    );
    setSqlConnectorConfig(sqlConnectorConfig);

    setPinotTable(_pinotTable);
    setPinotControllerUrl(_pinotControllerUrl);

    SqlQueryConfig sqlQueryConfig = new SqlQueryConfig(
        "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE O_ORDERDATE > $START AND O_ORDERDATE < $END",
        "yyyy-MM-dd",
        "O_ORDERDATE",
        12,
        "DAYS",
        "yyyy-MM-dd",
        "1995-01-01",
        "1995-01-20"
    );

    setSqlQueryConfig(sqlQueryConfig);
  }

  /**
   * For testing only. Cluster should already be created when running tool.
   */
  private void startCluster(String clusterName) throws Exception {
    _testPinotTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));

    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(2181);
    zkStarter.setDataDir(new File(_testPinotTmpDir, "PinotZkDir").getAbsolutePath());
    zkStarter.execute();

    DeleteClusterCommand deleteClusterCommand = new DeleteClusterCommand().setClusterName(clusterName);
    deleteClusterCommand.execute();

    StartControllerCommand controllerStarter =
        new StartControllerCommand().setControllerPort("9000")
            .setZkAddress("localhost:2181")
            .setClusterName(clusterName);

    controllerStarter.execute();

    StartBrokerCommand brokerStarter =
        new StartBrokerCommand().setClusterName(clusterName).setPort(Integer.valueOf("8000"));
    brokerStarter.execute();

    StartServerCommand serverStarter =
        new StartServerCommand().setPort(Integer.valueOf("7000")).setClusterName(clusterName);
    serverStarter.execute();
  }

  /**
   * For testing only. Actual table should already be present when running connector.
   */
  private void addTestTable()
      throws Exception {
    String tableConfigFile = getClass().getClassLoader().getResource("snowflake/snowflakeTestConfig.json").getFile();

    String schemaFile = getClass().getClassLoader().getResource("snowflake/snowflakeTestSchema.json").getFile();

    AddTableCommand addTableCommand =
          new AddTableCommand()
              .setControllerPort(_pinotControllerPort)
              .setControllerHost(_pinotControllerHost)
              .setSchemaFile(schemaFile)
              .setTableConfigFile(tableConfigFile).setExecute(true);

    addTableCommand.execute();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(_pinotTable)
        .setTimeColumnName("O_ORDERDATE")
        .setTimeType("days")
        .setNumReplicas(3)
        .setBrokerTenant("broker")
        .setServerTenant("server")
        .build();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(_pinotControllerUrl).forTableCreate(),
        tableConfig.toJsonString());
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
