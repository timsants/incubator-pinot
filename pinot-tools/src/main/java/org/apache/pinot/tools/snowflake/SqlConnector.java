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

import com.google.common.base.Preconditions;
import java.io.InputStream;
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
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.plugin.inputformat.parquet.ResultSetParquetTransformer;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.spec.ExecutionFrameworkSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SqlConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(SqlConnector.class);

  private static final Pattern COUNT_STAR_REGEX = Pattern.compile("(?i)(select)(.*)(from.*)");
  private static final String TMP_DIR_PREFIX = "sql_connector_data_";

  private SqlConnectorConfig _sqlConnectorConfig;
  private String _pinotTable;
  private String _pinotControllerUrl;
  private SqlQueryConfig _sqlQueryConfig;
  private ResultSetParquetTransformer _resultSetParquetTransformer;
  private Path _tmpDataDir;

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
    Preconditions.checkArgument(_sqlConnectorConfig != null, "SqlConnectorConfig not set");
    Preconditions.checkArgument(_sqlQueryConfig != null, "SqlQueryConfig not set");

    Statement statement = getJDBCConnection(_sqlConnectorConfig);

    _resultSetParquetTransformer = new ResultSetParquetTransformer();
    _tmpDataDir = Files.createTempDirectory(TMP_DIR_PREFIX);

    batchReadData(statement);

    buildAndPushSegments();

    statement.close();
  }

  abstract void verifyDriverPresent() throws IllegalStateException;

  private void batchReadData(Statement statement) throws Exception {
    DateTimeFormatter windowFormatter = new DateTimeFormatterBuilder()
        .appendPattern(_sqlQueryConfig.getWindowDateTimeFormat())
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter();

    LocalDateTime windowStart = LocalDateTime.parse(_sqlQueryConfig.getStartTime(), windowFormatter);
    LocalDateTime windowEnd = LocalDateTime.parse(_sqlQueryConfig.getEndTime(), windowFormatter);

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

      String chunkQuery = _sqlQueryConfig.getQueryTemplate()
          .replace("> $START", "> '" + convertDateTimeToDatabaseFormat(batchStart) + "'")
          .replace("< $END", "<= '" + convertDateTimeToDatabaseFormat(batchEnd) + "'");

      LOGGER.info("Chunk query: {}", chunkQuery);
      queryAndSaveChunks(statement, chunkQuery, chunkNum);

      batchStart = batchEnd; //TODO needs to be incremented since between is inclusive
      batchEnd = getNextGranularity(batchEnd);
      chunkNum++;
    }
  }

  private InputStream queryAndSaveChunks(Statement statement, String query, int chunkNum) throws Exception {
    LOGGER.info("Executing query: {}", query);
    ResultSet resultSet = statement.executeQuery(query);

    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    LOGGER.info("Number of columns = {}", resultSetMetaData.getColumnCount());

    for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
      LOGGER.info("Column {} : type={}", colIdx, resultSetMetaData.getColumnTypeName(colIdx + 1));
    }

    InputStream inputStream = _resultSetParquetTransformer
        .transform(resultSet, "test" , "username" + "." + "database", _tmpDataDir, Integer.toString(chunkNum));

    return inputStream;
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

  private LocalDateTime getNextGranularity(LocalDateTime dateTime) {
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

  private int getNumberOfRows(Statement statement, LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd) throws SQLException {
    // Generate count(*) query to get number of rows
    Matcher matcher = COUNT_STAR_REGEX.matcher(_sqlQueryConfig.getQueryTemplate());
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
