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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.StartBrokerCommand;
import org.apache.pinot.tools.admin.command.StartControllerCommand;
import org.apache.pinot.tools.admin.command.StartServerCommand;
import org.apache.pinot.tools.admin.command.StartZookeeperCommand;
import org.apache.pinot.tools.admin.command.StopProcessCommand;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;


public class SnowflakeConnector implements SqlConnector {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnector.class);

  private static final String TEMP_OUTPUT_DIR = "/tmp/snowflake";
  private static final Pattern COUNT_STAR_REGEX = Pattern.compile("(?i)(select)(.*)(from.*)");

  //make all of these database props - multivalue

  //Snowflake user name
  private String _username;
  @Option(name = "-password", required = true, metaVar = "<int>", usage = "Snowflake password")
  private String _password;
  @Option(name = "-account", required = true, metaVar = "<int>", usage = "Snowflake account name")
  private String _account;
  @Option(name = "-database", required = true, metaVar = "<int>", usage = "Snowflake database")
  private String _database;
  @Option(name = "-schema", required = true, metaVar = "<int>", usage = "Snowflake schema")
  private String _schema;
  @Option(name = "-table", required = true, metaVar = "<int>", usage = "Snowflake table")
  private String _table;
  @Option(name = "-controllerUrl", required = true, metaVar = "<int>", usage = "Pinot controller URL")
  private String _controllerUrl;

  @Option(name = "-queryTemplate", required = true, metaVar = "<int>", usage = "Templatized SQL query for pulling from Snowflake table")
  private String _queryTemplate;

  private File _tempDir;


  private String _timeColumnFormat; //format of time column expressed as date format. other accepted values are millisecondsSinceEpoch and secondsSinceEpoch.
  private String _timeColumnName; //name of column

  private long _dataPullAmount; //? do we need this
  private String _dataPullGranularity; //how big each chunk should be SECONDS, MINUTES, HOURS, DAYS

  private String _windowDateTimeFormat = "yyyy-MM-dd"; //optional; Format of startTime and endTime
  private String _startTime; //string ISO format or could add format...
  private String _endTime;
  @Option(name = "-pinotTable", required = true, metaVar = "<int>", usage = "Pinot table to import data into")
  private String _pinotTable;
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  private boolean _isStopped;

  private DateTimeFormatter _windowFormatter;


  public static void main(String args[]) throws Exception {
    SnowflakeConnector connector = new SnowflakeConnector();
    connector.init();
    connector.initTestEnv();
    connector.execute();
    connector.stop();
  }

  private void stop() throws Exception {
    if (_isStopped) {
      return;
    }

    StopProcessCommand stopper = new StopProcessCommand(false);
    stopper.stopController().stopBroker().stopServer().stopZookeeper();
    stopper.execute();
    FileUtils.cleanDirectory(_tempDir);

    _isStopped = true;
  }

  //TODO make into framework. snowflake impl.
  //parameters
  //sql format dialect

  private SnowflakeConnector initTestEnv() throws Exception {
    _username = "timsants";
    _password = "egh9SMUD!thuc*toom";
    _account = "xg65443.west-us-2.azure";

    _database = "SNOWFLAKE_SAMPLE_DATA";
    _schema = "TPCH_SF1";

    _queryTemplate = "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE O_ORDERDATE > $START AND O_ORDERDATE < $END";

    _startTime = "1995-01-01";
    _endTime = "1995-01-20";

    _dataPullGranularity = "DAYS"; //what if data pull amount is finer granularity than time format
    _dataPullAmount = 12;

    _timeColumnFormat = "yyyy-MM-dd";
    _timeColumnName = "O_ORDERDATE";

    _pinotTable = "snowflakeTest";


    startCluster("mycluster");

    return this;
  }

  /**
   * Validate provided paramters.
   */
  private void init() {
    // make sure _dataPullGranularity is not smaller than timeColumnFormatGranularity


    _windowFormatter = new DateTimeFormatterBuilder().appendPattern(_windowDateTimeFormat)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter();
  }

  private void startCluster(String clusterName) throws Exception {
    _tempDir = new File(FileUtils.getTempDirectory(), String.valueOf(System.currentTimeMillis()));

    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
    zkStarter.setPort(2181);
    zkStarter.setDataDir(new File(_tempDir, "PinotZkDir").getAbsolutePath());
    zkStarter.execute();

    DeleteClusterCommand deleteClusterCommand = new DeleteClusterCommand().setClusterName(clusterName);
    deleteClusterCommand.execute();

    StartControllerCommand controllerStarter =
        new StartControllerCommand().setControllerPort("9000").setZkAddress("localhost:2181")
            .setClusterName(clusterName);

    controllerStarter.execute();

    StartBrokerCommand brokerStarter =
        new StartBrokerCommand().setClusterName(clusterName).setPort(Integer.valueOf("8000"));
    brokerStarter.execute();

    StartServerCommand serverStarter =
        new StartServerCommand().setPort(Integer.valueOf("7000")).setClusterName(clusterName);
    serverStarter.execute();

    addTable();
  }

  private void addTable()
      throws Exception {
    String tableConfigFile = getClass().getClassLoader().getResource("snowflake/snowflakeTestConfig.json").getFile();

    String schemaFile = getClass().getClassLoader().getResource("snowflake/snowflakeTestSchema.json").getFile();

    AddTableCommand addTableCommand =
          new AddTableCommand().setControllerPort("9000").setSchemaFile(schemaFile)
              .setTableConfigFile(tableConfigFile).setExecute(true);

    addTableCommand.execute();

    String controllerAddress = "http://localhost:9000";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("snowflakeTest").setTimeColumnName(_timeColumnName)
            .setTimeType("days").setNumReplicas(3).setBrokerTenant("broker").setServerTenant("server").build();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(controllerAddress).forTableCreate(),
        tableConfig.toJsonString());
  }

  //@Override
  public boolean execute() throws Exception {
    // Read data in chunks
    batchReadData();

    // Generate segments and upload
    buildAndPushSegments();

    return true;
  }

  private void buildAndPushSegments() {
    ExecutionFrameworkSpec frameworkSpec = new ExecutionFrameworkSpec();
    frameworkSpec.setName("standalone");
    frameworkSpec.setSegmentGenerationJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentGenerationJobRunner");
    frameworkSpec.setSegmentTarPushJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner");
    frameworkSpec.setSegmentUriPushJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentUriPushJobRunner");

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setExecutionFrameworkSpec(frameworkSpec);
    spec.setJobType("SegmentCreationAndTarPush");
    spec.setInputDirURI(TEMP_OUTPUT_DIR);
    //spec.setIncludeFileNamePattern("glob:*.parquet");
    spec.setIncludeFileNamePattern("glob:" + TEMP_OUTPUT_DIR + "/*.json");

    spec.setOutputDirURI(TEMP_OUTPUT_DIR + "/segments");
    spec.setOverwriteOutput(true);

    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme("file");
    pinotFSSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    spec.setPinotFSSpecs(Collections.singletonList(pinotFSSpec));

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    /*
    recordReaderSpec.setDataFormat("parquet");
    recordReaderSpec.setClassName("org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
     */
    recordReaderSpec.setClassName("org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
    spec.setRecordReaderSpec(recordReaderSpec);

    TableSpec tableSpec = new TableSpec();
    tableSpec.setTableName(_pinotTable);
    tableSpec.setSchemaURI("http://localhost:9000/tables/" + _pinotTable + "/schema");
    tableSpec.setTableConfigURI("http://localhost:9000/tables/" + _pinotTable);
    spec.setTableSpec(tableSpec);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI("http://localhost:9000");
    spec.setPinotClusterSpecs(new PinotClusterSpec[]{pinotClusterSpec});

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(2);
    pushJobSpec.setPushRetryIntervalMillis(1000);
    spec.setPushJobSpec(pushJobSpec);

    IngestionJobLauncher.runIngestionJob(spec);
  }

  private LocalDateTime getNextGranularity(LocalDateTime dateTime) {
    switch (_dataPullGranularity) {
      case "SECONDS":
        return dateTime.plusSeconds(_dataPullAmount);
      case "MINUTES":
        return dateTime.plusMinutes(_dataPullAmount);
      case "HOURS":
        return dateTime.plusHours(_dataPullAmount);
      case "DAYS":
        return dateTime.plusDays(_dataPullAmount);
      case "WEEKS":
        return dateTime.plusWeeks(_dataPullAmount);
      case "MONTHS":
        return dateTime.plusMonths(_dataPullAmount);
      case "YEARS":
        return dateTime.plusYears(_dataPullAmount);
        default:
          throw new UnsupportedOperationException("Data pull granularity not support: " + _dataPullGranularity);
    }
  }

  private String convertDateTimeToDatabaseFormat(LocalDateTime dateTime) {
    switch (_timeColumnFormat) {
      case "millisecondsSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC) * 1000); //TODO which zone offset do we use
      case "secondsSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC));
      case "hoursSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC) / 60);
      default:
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(_timeColumnFormat);
        return dateTime.format(formatter);
    }
  }


  private void batchReadData() throws Exception {
    // Get JDBC connection
    Statement statement = getJDBCConnection();

    LocalDateTime windowStart = LocalDateTime.parse(_startTime, _windowFormatter);
    LocalDateTime windowEnd = LocalDateTime.parse(_endTime, _windowFormatter);

    // Get count(*) to determine total number of rows in each chunk
    int numRows = getNumberOfRows(statement, windowStart, windowEnd);


    LocalDateTime batchStart = windowStart;
    LocalDateTime batchEnd = getNextGranularity(windowStart);
    boolean isLastBatch = false;
    int chunkNum = 1;
    while (!isLastBatch) {
      //TODO: can make dynamic. if we can pull more rows if we need to. make another count(*) recursively.

      if (batchEnd.isAfter(windowEnd)) {
        batchEnd = windowEnd;
        isLastBatch = true;
      }

      String chunkQuery = _queryTemplate
          .replace("> $START", "> '" + convertDateTimeToDatabaseFormat(batchStart) + "'")
          .replace("< $END", "<= '" + convertDateTimeToDatabaseFormat(batchEnd) + "'");

      LOGGER.info("Chunk query: {}", chunkQuery);
      queryAndSaveChunks(statement, chunkQuery, chunkNum);

      batchStart = batchEnd; //TODO needs to be incremented since between is inclusive
      batchEnd = getNextGranularity(batchEnd);
      chunkNum++;
    }

    statement.close();
  }

  private int getNumberOfRows(Statement statement, LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd) throws SQLException {
    // Generate count(*) query to get number of rows
    Matcher matcher = COUNT_STAR_REGEX.matcher(_queryTemplate);
    matcher.find();
    String countQuery = matcher.group(1) + " COUNT(*) " + matcher.group(3);
    String countQueryWithTimeRange = countQuery.replace("$START", "'" + convertDateTimeToDatabaseFormat(dateTimeStart) + "'")
        .replace("< $END", "<= '" + convertDateTimeToDatabaseFormat(dateTimeEnd) + "'");

    LOGGER.info("Making query {}", countQueryWithTimeRange);
    ResultSet resultSet = statement.executeQuery(countQueryWithTimeRange);

    // fetch metadata
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    if (resultSetMetaData.getColumnCount() != 1) {
      throw new IllegalStateException("Expected 1 column in result set. Instead got " + resultSetMetaData.getColumnCount());
    }

    // fetch data
    resultSet.next();
    int rowCount = resultSet.getInt( 1);

    LOGGER.info("There are {} number of rows in the time range {} to {}", rowCount, dateTimeStart.toString(), dateTimeEnd.toString());

    return rowCount;
  }

  private void queryAndSaveChunks(Statement statement, String query, int chunkNum) throws Exception {
    LOGGER.info("Executing query: {}", query);
    ResultSet resultSet = statement.executeQuery(query);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    LOGGER.info("Number of columns = {}", resultSetMetaData.getColumnCount());

    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
      LOGGER.info("Column {} : type={}", colIdx, resultSetMetaData.getColumnTypeName(colIdx + 1));
    }

    File file = new File(TEMP_OUTPUT_DIR + "/chunk_"+ chunkNum + ".json");
    FileWriter fileWriter = new FileWriter(file);
    while (resultSet.next()) {

      // convert to parquet here and write to file
      ObjectMapper objectMapper = new ObjectMapper();
      ObjectNode objectNode = objectMapper.createObjectNode();
      for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
        objectNode.put(resultSetMetaData.getColumnName(colIdx + 1), resultSet.getString(colIdx + 1));
      }

      fileWriter.write(objectNode.toString() + "\n");
      LOGGER.info("Results: {}", objectNode.toString());
    }
    fileWriter.close();
  }

  private Statement getJDBCConnection() throws SQLException {
    LOGGER.debug("Creating JDBC connection...");
    Connection connection = getConnection();
    LOGGER.info("Done creating JDBC connection");
    // create statement
    LOGGER.info("Create JDBC statement");
    return connection.createStatement();
  }

  private Connection getConnection()
      throws SQLException
  {
    try
    {
      Class.forName("com.snowflake.client.jdbc.SnowflakeDriver");
    }
    catch (ClassNotFoundException ex)
    {
      LOGGER.error("Driver not found");
    }
    // build connection properties
    Properties properties = new Properties();
    properties.put("user", _username);     // TIMSANTS
    properties.put("password", _password); // "egh9SMUD!thuc*toom"
    properties.put("account", _account);  // "xg65443"
    properties.put("db", _database);       // "SNOWFLAKE_SAMPLE_DATA"
    properties.put("schema", _schema);   // "TPCH_SF001"
    //properties.put("tracing", "on");

    // create a new connection
    String connectStr = System.getenv("SF_JDBC_CONNECT_STRING");
    // use the default connection string if it is not set in environment
    if(connectStr == null)
    {
      connectStr = "jdbc:snowflake://" + _account + ".snowflakecomputing.com"; // replace accountName with your account name
    }
    return DriverManager.getConnection(connectStr, properties);
  }
}
