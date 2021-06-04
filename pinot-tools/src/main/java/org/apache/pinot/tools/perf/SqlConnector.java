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
package org.apache.pinot.tools.perf;

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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jdk.vm.ci.meta.Local;
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
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.AddTableCommand;
import org.apache.pinot.tools.admin.command.DeleteClusterCommand;
import org.apache.pinot.tools.admin.command.StartBrokerCommand;
import org.apache.pinot.tools.admin.command.StartControllerCommand;
import org.apache.pinot.tools.admin.command.StartServerCommand;
import org.apache.pinot.tools.admin.command.StartZookeeperCommand;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand.sendPostRequest;


public class SqlConnector extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlConnector.class);

  private static final int CHUNK_SIZE = 100000;
  private static final String TEMP_OUTPUT_DIR = "/tmp/snowflake";
  private static final Pattern COUNT_STAR_REGEX = Pattern.compile("(?i)(select)(.*)(from.*)");

  //make all of these database props - multivalue
  @Option(name = "-username", required = true, metaVar = "<int>", usage = "Snowflake user name")
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

  private String _timeColumnFormat; //format of time column expressed as date format. other accepted values are millisecondsSinceEpoch and secondsSinceEpoch.
  private String _timeColumnName; //name of column

  private long _dataPullAmount; //? do we need this
  private String _dataPullGranularity; //how big each chunk should be SECONDS, MINUTES, HOURS, DAYS

  private String _windowDateTimeFormat; //optional; Format of startTime and endTime
  private String _startTime; //string ISO format or could add format...
  private String _endTime;




  @Option(name = "-pinotTable", required = true, metaVar = "<int>", usage = "Pinot table to import data into")
  private String _pinotTable;
  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;


  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public String description() {
    return "Pinot tool snowflake connector\n";
  }

  public static void main(String args[]) throws Exception {


    new SqlConnector().initTestEnv().execute();
  }

  //TODO make into framework. snowflake impl.
  //parameters
  //sql format dialect

  private SqlConnector initTestEnv() throws Exception {
    _username = "timsants";
    _password = "egh9SMUD!thuc*toom";
    _account = "xg65443.west-us-2.azure";

    _database = "SNOWFLAKE_SAMPLE_DATA";
    _schema = "TPCH_SF1";

    _queryTemplate = "SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE FROM ORDERS WHERE O_ORDERDATE > '1995-01-01' ORDER BY O_ORDERKEY";

    _pinotTable = "snowflakeTest";

    startCluster("mycluster");

    return this;
  }

  private void startCluster(String clusterName) throws Exception {
    StartZookeeperCommand zkStarter = new StartZookeeperCommand();
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
    if (_tableConfigFile != null) {
      AddTableCommand addTableCommand =
          new AddTableCommand().setControllerPort(_controllerPort).setSchemaFile(_schemaFileName)
              .setTableConfigFile(_tableConfigFile).setExecute(true);
      addTableCommand.execute();
      return;
    }

    if (_tableName == null) {
      LOGGER.error("Table info not specified in configuration, please specify either config file or table name");
      return;
    }

    String controllerAddress = "http://" + _localhost + ":" + _controllerPort;
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(_tableName).setTimeColumnName(_timeColumnName)
            .setTimeType(_timeUnit).setNumReplicas(3).setBrokerTenant("broker").setServerTenant("server").build();
    sendPostRequest(ControllerRequestURLBuilder.baseUrl(controllerAddress).forTableCreate(),
        tableConfig.toJsonString());
  }

  @Override
  public boolean execute() throws Exception {
    // Read data in chunks
    readDataFromSnowflake();

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
    spec.setIncludeFileNamePattern("glob:*.parquet");
    spec.setOutputDirURI(TEMP_OUTPUT_DIR + "/segments");
    spec.setOverwriteOutput(true);

    PinotFSSpec pinotFSSpec = new PinotFSSpec();
    pinotFSSpec.setScheme("file");
    pinotFSSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
    spec.setPinotFSSpecs(Collections.singletonList(pinotFSSpec));

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setDataFormat("parquet");
    recordReaderSpec.setClassName("org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
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

  private void readDataFromSnowflake() throws Exception {
    // Get JDBC connection
    Statement statement = getJDBCConnection();
    // Get count(*) to determine total number of rows in each chunk
    int numRows = getNumberOfRows(statement);
    int numChunks = numRows / CHUNK_SIZE + 1; // Add 1 since division rounds down

    LOGGER.info("Will be fetching {} number of rows and {} chunks", numRows, numChunks);

    if (_windowDateTimeFormat == null) {
      _windowDateTimeFormat = "yyyy-MM-dd"; //use ISO format
    }
    DateTimeFormatter windowFormatter = DateTimeFormatter.ofPattern(_windowDateTimeFormat);
    LocalDateTime windowStart = LocalDateTime.parse(_startTime, windowFormatter);
    LocalDateTime windowEnd = LocalDateTime.parse(_endTime, windowFormatter);

    LocalDateTime nextBatchEnd = getNextGranularity(windowStart);
    boolean isLastBatch = false;
    while (!isLastBatch) {
      if (nextBatchEnd.isAfter(windowEnd)) {
        nextBatchEnd = windowEnd;
        isLastBatch = true;
      }

      //make dynamic. if we can pull more rows if we need to. make another count(*) recursively.
      _queryTemplate
      int offset = i * CHUNK_SIZE;
      int limit = CHUNK_SIZE;
      if (offset + CHUNK_SIZE > numRows) {
        limit = numRows - offset;
      }


      String chunkQuery = _queryTemplate + " LIMIT " + limit + " OFFSET " + offset; //dont do this. need order by.
      LOGGER.info("Chunk query: {}", chunkQuery);
      queryAndSaveChunks(statement, chunkQuery, i);
    }

    statement.close();
  }

  private int getNumberOfRows(Statement statement) throws SQLException {
    // Generate count(*) query to get number of rows
    Matcher matcher = COUNT_STAR_REGEX.matcher(_queryTemplate);
    matcher.find();
    String countQuery = matcher.group(1) + " COUNT(*) " + matcher.group(3);

    ResultSet resultSet = statement.executeQuery(countQuery);

    // fetch metadata
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    if (resultSetMetaData.getColumnCount() != 1) {
      throw new IllegalStateException("Expected 1 column in result set. Instead got " + resultSetMetaData.getColumnCount());
    }

    // fetch data
    resultSet.next();
    int rowCount = resultSet.getInt( 1);

    return rowCount;
  }

  private void queryAndSaveChunks(Statement statement, String query, int chunkNum) throws Exception {
    // make query
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
