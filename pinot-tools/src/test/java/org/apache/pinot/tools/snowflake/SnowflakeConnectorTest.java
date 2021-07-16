package org.apache.pinot.tools.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.ZkStarter;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push.SqlConnectorBatchPushTaskExecutor;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.PostQueryCommand;
import org.apache.pinot.tools.admin.command.StartBrokerCommand;
import org.apache.pinot.tools.admin.command.StartControllerCommand;
import org.apache.pinot.tools.admin.command.StartMinionCommand;
import org.apache.pinot.tools.admin.command.StartServerCommand;
import org.apache.pinot.tools.admin.command.StartZookeeperCommand;
import org.mockito.internal.util.collections.Sets;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import static org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;
import static org.testng.Assert.assertEquals;

/**
 * Tests for {@link SnowflakeConnector}
 */
public class SnowflakeConnectorTest {

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectorTest.class);

  private File _testPinotTmpDir;

  private static final String PINOT_TABLE = "snowflakeTest";
  private static final String CLUSTER_NAME = "testCluster";
/*
  private static final String PINOT_CONTROLLER_HOST = "pinot87d28eda.itpinot76696eaf.192.168.64.176.nip.io";
  private static final String PINOT_CONTROLLER_PORT = "30000";
  */
  private static final String PINOT_CONTROLLER_HOST = "localhost";
  private static final String PINOT_CONTROLLER_PORT = "9000";
  private static final String PINOT_CONTROLLER_URL = "http://" + PINOT_CONTROLLER_HOST + ":" + PINOT_CONTROLLER_PORT;

  private static final String TABLE_CONFIG_FILE = "snowflake/snowflakeTestConfig.json";
  private static final String SCHEMA_FILE = "snowflake/snowflakeTestSchema.json";

  @Test
  public void testWithBatchConfig() throws Exception {
    startPinotCluster();
    createPinotTable();

    SqlConnectorConfig sqlConnectorConfig = new SnowflakeConfig(
        "startree",
        "egh9SMUD!thuc*toom",
        "vka51661",
        "SNOWFLAKE_SAMPLE_DATA",
        "TPCH_SF1",
        "ORDERS"
    );
    SqlQueryConfig sqlQueryConfig = new SqlQueryConfig(
        "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE "
            + "O_ORDERDATE BETWEEN $START AND $END",
        "yyyy-MM-dd",
        "O_ORDERDATE",
        "yyyy-MM-dd",
        "1992-01-01",
        "1999-01-01",
        new SqlQueryConfig.BatchQueryConfig(3L, "YEARS")
    );

    SnowflakeConnector connector = new SnowflakeConnector(sqlConnectorConfig, sqlQueryConfig, PINOT_CONTROLLER_URL, PINOT_TABLE);
    connector.execute();

    LOGGER.info("Waiting for documents to load...");
    Thread.sleep(10000);

    long numRowsInSnowflake = connector.getTotalNumberOfRows(connector.getJDBCConnection());
    long numRowsInPinot = runPinotQuery("select count(*) from " + PINOT_TABLE)
        .get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(numRowsInSnowflake, numRowsInPinot);

    Set<String> pinotColumns = JsonUtils.jsonNodeToObject(
        runPinotQuery("select * from " + PINOT_TABLE)
            .get("resultTable")
            .get("dataSchema")
            .get("columnNames"),
        Set.class);
    assertEquals(pinotColumns,
        Sets.newSet("o_custkey","o_orderdate","o_orderkey","o_orderstatus","o_totalprice"));
  }

  @Test
  public void testWithoutBatchConfig() throws Exception {
    startPinotCluster();
    createPinotTable();
    SqlConnectorBatchPushTaskExecutor executor = new SqlConnectorBatchPushTaskExecutor();
    File localTempDir = new File(new File(MinionContext.getInstance().getDataDir(), "SegmentGenerationAndPushResult"),
        "tmp-" + UUID.randomUUID());
    //executor.generateTaskSpec(localTempDir);
    /*
    SqlConnectorConfig sqlConnectorConfig = new SnowflakeConfig(
        "startree",
        "egh9SMUD!thuc*toom",
        "vka51661",
        "SNOWFLAKE_SAMPLE_DATA",
        "TPCH_SF1",
        "ORDERS"
    );
    SqlQueryConfig sqlQueryConfig = new SqlQueryConfig(
        "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE "
            + "O_ORDERDATE BETWEEN $START AND $END",
        "yyyy-MM-dd",
        "O_ORDERDATE",
        "yyyy-MM-dd",
        "1992-01-01",
        "1999-01-01",
        null
    );

    SnowflakeConnector connector = new SnowflakeConnector(sqlConnectorConfig, sqlQueryConfig, PINOT_CONTROLLER_URL, PINOT_TABLE);
    connector.execute();

    LOGGER.info("Waiting for documents to load...");
    Thread.sleep(10000);

    long numRowsInSnowflake = connector.getTotalNumberOfRows(connector.getJDBCConnection());
    long numRowsInPinot = runPinotQuery("select count(*) from " + PINOT_TABLE)
        .get("resultTable").get("rows").get(0).get(0).asLong();

    //TODO use TestUtils.waitForCondition
    assertEquals(numRowsInSnowflake, numRowsInPinot);

    Set<String> pinotColumns = JsonUtils.jsonNodeToObject(
        runPinotQuery("select * from " + PINOT_TABLE)
            .get("resultTable")
            .get("dataSchema")
            .get("columnNames"),
        Set.class);
    assertEquals(pinotColumns,
        Sets.newSet("o_custkey","o_orderdate","o_orderkey","o_orderstatus","o_totalprice"));
*/
    Thread.sleep(600000);
  }

  public JsonNode runPinotQuery(String query) throws Exception {
    return JsonUtils.stringToJsonNode(
        new PostQueryCommand()
            .setBrokerPort("8000")
            .setQueryType(CommonConstants.Broker.Request.SQL)
            .setQuery(query)
            .run()
    );
  }

  protected String getHelixClusterName() {
    return getClass().getSimpleName();
  }


  /**
   * For testing only. Cluster should already be created when running tool.
   */
  private void startPinotCluster() throws Exception {
    _testPinotTmpDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));

    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(2181);
    zkStarter.setDataDir(new File(_testPinotTmpDir, "PinotZkDir").getAbsolutePath());
    zkStarter.execute();

    DeleteClusterCommand deleteClusterCommand = new DeleteClusterCommand().setClusterName(CLUSTER_NAME);
    deleteClusterCommand.execute();

    StartControllerCommand controllerStarter =
        new StartControllerCommand().setControllerPort("9000")
            .setZkAddress("localhost:2181")
            .setClusterName(CLUSTER_NAME);

    controllerStarter.execute();

    StartBrokerCommand brokerStarter =
        new StartBrokerCommand().setClusterName(CLUSTER_NAME).setPort(Integer.valueOf("8000"));
    brokerStarter.execute();

    StartServerCommand serverStarter =
        new StartServerCommand().setPort(Integer.valueOf("7000")).setClusterName(CLUSTER_NAME);
    serverStarter.execute();

    //start minion
    StartMinionCommand minionStarter = new StartMinionCommand();
    minionStarter.execute();
    /*
    FileUtils.deleteQuietly(new File(CommonConstants.Minion.DEFAULT_INSTANCE_BASE_DIR));
    try {
      PinotConfiguration minionConf = new PinotConfiguration();
      minionConf.setProperty(CommonConstants.Helix.CONFIG_OF_CLUSTER_NAME, getHelixClusterName());
      minionConf.setProperty(CommonConstants.Helix.CONFIG_OF_ZOOKEEPR_SERVER, zookeeperInstance.getZkUrl());
      MinionStarter minionStarter = new MinionStarter();
      minionStarter.init(minionConf);
      minionStarter.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }*/
  }

  /**
   * For testing only. Actual table should already be present when running connector.
   */
  private void createPinotTable() throws Exception {
    String tableConfigFile = getClass().getClassLoader().getResource(TABLE_CONFIG_FILE).getFile();

    String schemaFile = getClass().getClassLoader().getResource(SCHEMA_FILE).getFile();

    AddTableCommand addTableCommand =
        new AddTableCommand()
            .setControllerPort(PINOT_CONTROLLER_PORT)
            .setControllerHost(PINOT_CONTROLLER_HOST)
            .setSchemaFile(schemaFile)
            .setTableConfigFile(tableConfigFile).setExecute(true);

    addTableCommand.execute();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(PINOT_TABLE)
        .setTimeColumnName("O_ORDERDATE")
        .setTimeType("days")
        .setNumReplicas(3)
        .setBrokerTenant("broker")
        .setServerTenant("server")
        .build();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(PINOT_CONTROLLER_URL).forTableCreate(),
        tableConfig.toJsonString());
  }

  @AfterTest
  private void stopCluster() throws Exception {
    if (_testPinotTmpDir != null) {
      FileUtils.cleanDirectory(_testPinotTmpDir);
    }
  }
}
