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
package org.apache.pinot.integration.tests.plugin.minion.tasks;

import com.fasterxml.jackson.databind.JsonNode;
import groovy.lang.IntRange;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.io.FileUtils;
import org.apache.helix.task.TaskConfig;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.Request;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.integration.tests.BaseClusterIntegrationTest;
import org.apache.pinot.integration.tests.BasicAuthTestUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.BasicAuthTestUtils.AUTH_HEADER;
import static org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;


public class SqlConnectorBatchPushTaskTest extends BaseClusterIntegrationTest {

  private static final String TABLE_CONFIG_FILE = "snowflake/snowflakeMinionTestConfig.json";
  private static final String SCHEMA_FILE = "snowflake/snowflakeMinionTestSchema.json";
  private static final String PINOT_TABLE = "snowflakeTest";

  private static final String PINOT_CONTROLLER_URL = "http://" + "localhost" + ":" + DEFAULT_CONTROLLER_PORT;


  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start Zookeeper
    startZk();
    // Start Pinot cluster
    startKafka();
    startController();
    startBroker();
    startServer();
    startMinion();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(avroFiles.get(0)));
    addTableConfig(createOfflineTableConfig());

    createPinotTable();
  }

  /**
   * For testing only. Actual table should already be present when running connector.
   */
  private void createPinotTable() throws Exception {

    URI schemaFile = getClass().getClassLoader().getResource(SCHEMA_FILE).toURI();
    Schema schema = Schema.fromFile(new File(schemaFile));
    addSchema(schema);

    URI tableConfigFile = getClass().getClassLoader().getResource(TABLE_CONFIG_FILE).toURI();

    String tableConfig = JsonUtils.fileToJsonNode(new File(tableConfigFile)).toString();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig, AUTH_HEADER);


/*
    AddTableCommand addTableCommand =
        new AddTableCommand()
            .setControllerPort(Integer.toString(DEFAULT_CONTROLLER_PORT))
            .setControllerHost(Integer.toString(DEFAULT_CONTROLLER_PORT))
            .setSchemaFile(schemaFile)
            .setTableConfigFile(tableConfigFile).setExecute(true);

    addTableCommand.execute();

    Map<String, String> batchConfigMap = new HashMap<>();
    "className": "org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push.SnowflakeConnectorPlugin",
        "properties.username": "startree",
        "properties.password": "egh9SMUD!thuc*toom",
        "properties.account": "vka51661",
        "properties.db": "SNOWFLAKE_SAMPLE_DATA",
        "properties.schema": "TPCH_SF1",
        "queryTemplate": "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE O_ORDERDATE BETWEEN $START AND $END",
        "timeColumnFormat": "yyyy-MM-dd",
        "timeColumnName": "O_ORDERDATE",
        "startEndTimeFormat": "yyyy-MM-dd",
        "startTime": "1992-01-01",
        "endTime": "1999-01-01"


    BatchIngestionConfig batchIngestionConfig = new BatchIngestionConfig(batchConfigMap, null, null);
    IngestionConfig ingestionConfig = new IngestionConfig(batchIngestionConfig);

    Map<String, Map<String, String>> taskTypeConfigsMap = new HashMap<>();
    taskTypeConfigsMap.put("SqlConnectorBatchPushTask", new HashMap<>(0));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(PINOT_TABLE)
        .setTimeColumnName("O_ORDERDATE")
        .setTimeType("days")
        .setNumReplicas(3)
        .setBrokerTenant("broker")
        .setServerTenant("server")
        .setIngestionConfig(ingestionConfig)
        .setTaskConfig(new TableTaskConfig(taskTypeConfigsMap))
        .build();

 */

  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopMinion();
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
/*
  @Override
  public Map<String, Object> getDefaultControllerConfiguration() {
    return BasicAuthTestUtils.addControllerConfiguration(super.getDefaultControllerConfiguration());
  }

  @Override
  protected PinotConfiguration getDefaultBrokerConfiguration() {
    return BasicAuthTestUtils.addBrokerConfiguration(super.getDefaultBrokerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultServerConfiguration() {
    return BasicAuthTestUtils.addServerConfiguration(super.getDefaultServerConfiguration().toMap());
  }

  @Override
  protected PinotConfiguration getDefaultMinionConfiguration() {
    return BasicAuthTestUtils.addMinionConfiguration(super.getDefaultMinionConfiguration().toMap());
  }

 */

  @Override
  protected TableTaskConfig getTaskConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("bucketTimePeriod", "30d");

    return new TableTaskConfig(
        Collections.singletonMap(MinionConstants.RealtimeToOfflineSegmentsTask.TASK_TYPE, properties));
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected void addSchema(Schema schema)
      throws IOException {
    PostMethod response =
        sendMultipartPostRequest(_controllerRequestURLBuilder.forSchemaCreate(), schema.toSingleLineJsonString(),
            AUTH_HEADER);
    Assert.assertEquals(response.getStatusCode(), 200);
  }

  @Override
  protected void addTableConfig(TableConfig tableConfig)
      throws IOException {
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonString(), AUTH_HEADER);
  }

  @Override
  protected Connection getPinotConnection() {
    if (_pinotConnection == null) {
      JsonAsyncHttpPinotClientTransportFactory factory = new JsonAsyncHttpPinotClientTransportFactory();
      factory.setHeaders(AUTH_HEADER);

      _pinotConnection =
          ConnectionFactory.fromZookeeper(getZkUrl() + "/" + getHelixClusterName(), factory.buildTransport());
    }
    return _pinotConnection;
  }

  @Override
  protected void dropRealtimeTable(String tableName)
      throws IOException {
    sendDeleteRequest(
        _controllerRequestURLBuilder.forTableDelete(TableNameBuilder.REALTIME.tableNameWithType(tableName)),
        AUTH_HEADER);
  }

  @Test
  public void testSegmentUploadDownload()
      throws Exception {
    Thread.sleep(600000);
  }
}
