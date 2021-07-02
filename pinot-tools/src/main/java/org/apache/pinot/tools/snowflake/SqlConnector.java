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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import groovy.sql.Sql;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.pinot.plugin.inputformat.parquet.ResultSetParquetTransformer;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.mortbay.util.SingletonList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SqlConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlConnector.class);

  private static final String TMP_DIR_PREFIX = "sql_connector_data_";

  private SqlConnectorConfig _sqlConnectorConfig;
  private String _pinotTable;
  private String _pinotControllerUrl;
  private SqlQueryConfig _sqlQueryConfig;
  private ResultSetParquetTransformer _resultSetParquetTransformer;
  private Path _tmpDataDir;
  private Function<LocalDateTime, String> _dateTimeToDBFormatConverter;


  protected void setSqlConnectorConfig(SqlConnectorConfig sqlConnectorConfig) {
    _sqlConnectorConfig = sqlConnectorConfig;
  }

  protected void setSqlQueryConfig(SqlQueryConfig sqlQueryConfig) {
    _sqlQueryConfig = sqlQueryConfig;
  }

  protected void setPinotTable(String pinotTable) {
    _pinotTable = pinotTable;
  }

  protected void setPinotControllerUrl(String pinotControllerUrl) {
    _pinotControllerUrl = pinotControllerUrl;
  }

  protected Statement getJDBCConnection() throws SQLException {
    verifyDriverPresent();

    LOGGER.info("Creating JDBC connection");
    Connection connection =  DriverManager.getConnection(
        _sqlConnectorConfig.getConnectString(),
        _sqlConnectorConfig.getConnectProperties()
    );

    LOGGER.info("Done creating JDBC connection");
    return connection.createStatement();
  }

  public void execute() throws Exception {
    Preconditions.checkArgument(_sqlConnectorConfig != null, "SqlConnectorConfig not set");
    Preconditions.checkArgument(_sqlQueryConfig != null, "SqlQueryConfig not set");


    _resultSetParquetTransformer = new ResultSetParquetTransformer();
    _tmpDataDir = Files.createTempDirectory(TMP_DIR_PREFIX);
    _dateTimeToDBFormatConverter = getDateTimeToDatabaseFormatConverter();

    Statement statement = getJDBCConnection();
    batchReadData(statement);
    statement.close();

    buildAndPushSegments();

  }

  abstract void verifyDriverPresent() throws IllegalStateException;

  private void batchReadData(Statement statement) throws Exception {
    LocalDateTime windowStart = _sqlQueryConfig.getWindowStartTime();

    // Get count(*) to determine total number of rows in each chunk
    long numRows = getNumberOfRows(statement, windowStart, _sqlQueryConfig.getWindowEndTime());



    SqlNode sqlNode = SqlParser.create(_sqlQueryConfig.getQueryTemplate(), SqlParser.config()).parseQuery();
    if (!sqlNode.isA(Set.of(SqlKind.SELECT))) {
      throw new IllegalArgumentException("Invalid query. Must provide a SELECT sql statement");
    }

    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    SqlBasicCall dateRangeNode = findBetweenOperator(sqlSelect.getWhere());


    LocalDateTime batchStart = windowStart;
    LocalDateTime batchEnd = getNextBatchEnd(windowStart);
    int chunkNum = 1;
    while (true) {
      //TODO: can make dynamic. if we can pull more rows if we need to. make another count(*) recursively.
      /*

      String chunkQuery = _sqlQueryConfig.getQueryTemplate()
          .replace("> $START", "> '" + _dateTimeToDBFormatConverter.apply(batchStart) + "'")
          .replace("< $END", "<= '" + _dateTimeToDBFormatConverter.apply(batchEnd) + "'");
*/
      String chunkQuery = replaceQueryStartAndEnd(sqlSelect, dateRangeNode,
          _dateTimeToDBFormatConverter.apply(batchStart),
          _dateTimeToDBFormatConverter.apply(batchEnd));
      queryAndSaveChunks(statement, chunkQuery, chunkNum);

      if (batchEnd == _sqlQueryConfig.getWindowEndTime()) {
        break;
      }

      batchStart = batchEnd; //TODO needs to be incremented since between is inclusive
      batchEnd = getNextBatchEnd(batchEnd);
      chunkNum++;
    }
  }

  private String replaceQueryStartAndEnd(SqlSelect sqlSelect, SqlBasicCall dateRangeNode, String startReplace,
      String endReplace) {
    ((SqlIdentifier) dateRangeNode.getOperands()[1])
        .setNames(Collections.singletonList("'" + startReplace + "'"), null);
    ((SqlIdentifier) dateRangeNode.getOperands()[2])
        .setNames(Collections.singletonList("'" + endReplace + "'"), null);

   String chunkQuery = sqlSelect.toSqlString(new SnowflakeSqlDialect(SnowflakeSqlDialect.EMPTY_CONTEXT))
        .getSql().replace("ASYMMETRIC ", ""); //TODO fix THIS!!
    LOGGER.info("New query with replaced dates is {}", chunkQuery);
    return chunkQuery;
  }

  private void queryAndSaveChunks(Statement statement, String query, int chunkNum) throws Exception {
    LOGGER.info("Executing query: {}", query);
    ResultSet resultSet = statement.executeQuery(query);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    LOGGER.info("Number of columns = {}", resultSetMetaData.getColumnCount());

    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
      LOGGER.info("Column {} : type={}", colIdx, resultSetMetaData.getColumnTypeName(colIdx + 1));
    }

    _resultSetParquetTransformer.transform(
        resultSet,
        "test" ,
        "username" + "." + "database",
        _tmpDataDir,
        Integer.toString(chunkNum)
    );
  }

  private String convertDateTimeToDatabaseFormat(LocalDateTime dateTime) {
    String timeColumnFormat = _sqlQueryConfig.getTimeColumnFormat();
    switch (timeColumnFormat) {
      case "millisecondsSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC) * 1000); //TODO which zone offset do we use
      case "secondsSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC));
      case "hoursSinceEpoch":
        return Long.toString(dateTime.toEpochSecond(ZoneOffset.UTC) / 60);
      default:
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeColumnFormat);
        return dateTime.format(formatter);
    }
  }

  private Function<LocalDateTime, String> getDateTimeToDatabaseFormatConverter() {
    String timeColumnFormat = _sqlQueryConfig.getTimeColumnFormat();
    switch (timeColumnFormat) {
      case "millisecondsSinceEpoch":
        return localDateTime -> Long.toString(localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000); //TODO which zone offset do we use
      case "secondsSinceEpoch":
        return localDateTime-> Long.toString(localDateTime.toEpochSecond(ZoneOffset.UTC));
      case "hoursSinceEpoch":
        return localDateTime -> Long.toString(localDateTime.toEpochSecond(ZoneOffset.UTC) / 60);
      default:
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(timeColumnFormat);
        return localDateTime -> localDateTime.format(formatter);
    }
  }

  private LocalDateTime getNextBatchEnd(LocalDateTime dateTime) {
    LocalDateTime nextDateTime = getNextDataPullDateTime(dateTime);
    if (nextDateTime.isAfter(_sqlQueryConfig.getWindowEndTime())) {
      nextDateTime = _sqlQueryConfig.getWindowEndTime();
    }
    return nextDateTime;
  }

  private LocalDateTime getNextDataPullDateTime(LocalDateTime dateTime) {
    long dataPullAmount = _sqlQueryConfig.getDataPullAmount();
    String dataPullGranularity = _sqlQueryConfig.getDataPullGranularity();

    switch (dataPullGranularity) {
      case "SECONDS":
        return dateTime.plusSeconds(dataPullAmount);
      case "MINUTES":
        return dateTime.plusMinutes(dataPullAmount);
      case "HOURS":
        return dateTime.plusHours(dataPullAmount);
      case "DAYS":
        return dateTime.plusDays(dataPullAmount);
      case "WEEKS":
        return dateTime.plusWeeks(dataPullAmount);
      case "MONTHS":
        return dateTime.plusMonths(dataPullAmount);
      case "YEARS":
        return dateTime.plusYears(dataPullAmount);
      default:
        throw new UnsupportedOperationException("Data pull granularity not support: " + dataPullGranularity);
    }
  }


  @VisibleForTesting
  protected long getTotalNumberOfRows(Statement statement) throws Exception {
    return getNumberOfRows(statement, _sqlQueryConfig.getWindowStartTime(), _sqlQueryConfig.getWindowEndTime());
  }

  private SqlBasicCall findBetweenOperator(SqlNode sqlNode) {
    if (sqlNode.getKind() == SqlKind.BETWEEN) {
      SqlBasicCall betweenCall = (SqlBasicCall) sqlNode;
      SqlNode columnName = betweenCall.getOperands()[0];
      SqlNode left = betweenCall.getOperands()[1];
      SqlNode right = betweenCall.getOperands()[2];

      if (columnName.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) columnName).names.get(0).equals(_sqlQueryConfig.getTimeColumnName())
          && left.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) left).names.get(0).equals(SqlQueryConfig.START)
          && right.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) right).names.get(0).equals(SqlQueryConfig.END)) {
        return betweenCall;
      }
    }

    if (sqlNode instanceof SqlCall) {
      for (SqlNode node : ((SqlBasicCall) sqlNode).getOperandList()) {
        return findBetweenOperator(node);
      }
    }

    throw new IllegalArgumentException("No between operator found!");
  }

  private long getNumberOfRows(Statement statement, LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd)
      throws Exception {
    // Generate count(*) query to get number of rows)

    SqlNode sqlNode = SqlParser.create(_sqlQueryConfig.getQueryTemplate(), SqlParser.config()).parseQuery();

    if (!sqlNode.isA(Set.of(SqlKind.SELECT))) {
      throw new IllegalArgumentException("Invalid query. Must provide a SELECT sql statement");
    }

    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    SqlBasicCall dateRangeNode = findBetweenOperator(sqlSelect.getWhere());


    //Set start and end identifiers
    ((SqlIdentifier) dateRangeNode.getOperands()[1])
        .setNames(Collections.singletonList("'" + convertDateTimeToDatabaseFormat(dateTimeStart) + "'"), null);
    ((SqlIdentifier) dateRangeNode.getOperands()[2])
        .setNames(Collections.singletonList("'" + convertDateTimeToDatabaseFormat(dateTimeEnd) + "'"), null);


    // Just choose first column in select which is used to make a SELECT COUNT(XXX) query
    SqlNode selectNodeFirst = sqlSelect.getSelectList().get(0);
    SqlCall countCall = SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, selectNodeFirst);
    sqlSelect.setSelectList(SqlNodeList.of(countCall));

    String countQueryWithTime = replaceQueryStartAndEnd(sqlSelect,
        dateRangeNode,
        convertDateTimeToDatabaseFormat(dateTimeStart),
        convertDateTimeToDatabaseFormat(dateTimeEnd)
    );

    LOGGER.info("Making count query:" + countQueryWithTime);

    /*
    String countQueryWithTimeRange = countQuery.replace("$START", "'" + convertDateTimeToDatabaseFormat(dateTimeStart) + "'")
        .replace("< $END", "<= '" + convertDateTimeToDatabaseFormat(dateTimeEnd) + "'");
*/
    LOGGER.info("Making query {}", countQueryWithTime);
    ResultSet resultSet = statement.executeQuery(countQueryWithTime);

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

  private void buildAndPushSegments() {
    Preconditions.checkArgument(_pinotControllerUrl != null, "Pinot controller URL must be set");
    Preconditions.checkArgument(_pinotTable != null, "Pinot table must be set");

    ExecutionFrameworkSpec frameworkSpec = new ExecutionFrameworkSpec();
    frameworkSpec.setName("standalone");
    frameworkSpec.setSegmentGenerationJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentGenerationJobRunner");
    frameworkSpec.setSegmentTarPushJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentTarPushJobRunner");
    frameworkSpec.setSegmentUriPushJobRunnerClassName("org.apache.pinot.plugin.ingestion.batch.standalone.SegmentUriPushJobRunner");

    SegmentGenerationJobSpec spec = new SegmentGenerationJobSpec();
    spec.setExecutionFrameworkSpec(frameworkSpec);
    spec.setJobType("SegmentCreationAndTarPush");
    spec.setInputDirURI(_tmpDataDir.toString());
    spec.setIncludeFileNamePattern("glob:" + _tmpDataDir.toString() + "/*.parquet");

    spec.setOutputDirURI(_tmpDataDir.toString() + "/segments");
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
    tableSpec.setSchemaURI(_pinotControllerUrl + "/tables/" + _pinotTable + "/schema");
    tableSpec.setTableConfigURI(_pinotControllerUrl + "/tables/" + _pinotTable);
    spec.setTableSpec(tableSpec);

    PinotClusterSpec pinotClusterSpec = new PinotClusterSpec();
    pinotClusterSpec.setControllerURI(_pinotControllerUrl);
    spec.setPinotClusterSpecs(new PinotClusterSpec[]{pinotClusterSpec});

    PushJobSpec pushJobSpec = new PushJobSpec();
    pushJobSpec.setPushAttempts(2);
    pushJobSpec.setPushRetryIntervalMillis(1000);
    spec.setPushJobSpec(pushJobSpec);

    IngestionJobLauncher.runIngestionJob(spec);
  }
}
