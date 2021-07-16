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
package org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push;

import com.google.common.base.Preconditions;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.DateString;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.generator.PinotTaskGenerator;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push.SnowflakeConnectorPlugin.getJDBCConnection;
import static org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push.StartreeMinionConstants.STARTREE_MINION_TASK;


/**
 * SegmentGenerationAndPushTaskGenerator generates task configs for SegmentGenerationAndPush minion tasks.
 *
 * This generator consumes configs from org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig:
 *   inputDirURI - Required, the location of input data directory
 *   inputFormat - Required, the input file format, e.g. JSON/Avro/Parquet/CSV/...
 *   input.fs.className - Optional, the class name of filesystem to read input data. Default to be inferred from inputDirURI if not specified.
 *   input.fs.prop.<keys> - Optional, defines the configs to initialize input filesystem.
 *
 *   outputDirURI - Optional, the location of output segments. Use local temp dir with push mode TAR, If not specified.
 *   output.fs.className - Optional, the class name of filesystem to write output segments. Default to be inferred from outputDirURI if not specified.
 *   output.fs.prop.<keys> - Optional, the configs to initialize output filesystem.
 *   overwriteOutput - Optional, delete the output segment directory if set to true.
 *
 *   recordReader.className - Optional, the class name of RecordReader. Default to be inferred from inputFormat if not specified.
 *   recordReader.configClassName - Optional, the class name of RecordReaderConfig. Default to be inferred from inputFormat if not specified.
 *   recordReader.prop.<keys> - Optional, the configs used to initialize RecordReaderConfig.
 *
 *   schema - Optional, Pinot schema in Json string.
 *   schemaURI - Optional, the URI to query for Pinot schema.
 *
 *   segmentNameGenerator.type - Optional, the segment name generator to create segment name.
 *   segmentNameGenerator.configs.<keys> - Optional, configs of segment name generator.
 *
 *   push.mode - Optional, push job type: TAR/URI/METADATA, default to METADATA
 *   push.controllerUri - Optional, controller uri to send push request to, default to the controller vip uri.
 *   push.segmentUriPrefix - Optional, segment download uri prefix, used when push.mode=uri.
 *   push.segmentUriSuffix - Optional, segment download uri suffix, used when push.mode=uri.
 *
 */
@TaskGenerator
public class SqlConnectorBatchPushTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SqlConnectorBatchPushTaskGenerator.class);
  private static final BatchConfigProperties.SegmentPushType DEFAULT_SEGMENT_PUSH_TYPE =
      BatchConfigProperties.SegmentPushType.TAR;

  private ClusterInfoAccessor _clusterInfoAccessor;

  private static final long DEFAULT_BATCH_NUM_ROWS = 1000000;

  @Override
  public void init(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public String getTaskType() {
    return STARTREE_MINION_TASK;
  }

  @Override
  public int getNumConcurrentTasksPerInstance() {
    String numConcurrentTasksPerInstanceStr = _clusterInfoAccessor
        .getClusterConfig(MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE);
    if (numConcurrentTasksPerInstanceStr != null) {
      try {
        return Integer.parseInt(numConcurrentTasksPerInstanceStr);
      } catch (Exception e) {
        LOGGER.error("Failed to parse cluster config: {}",
            MinionConstants.SegmentGenerationAndPushTask.CONFIG_NUMBER_CONCURRENT_TASKS_PER_INSTANCE, e);
      }
    }
    return JobConfig.DEFAULT_NUM_CONCURRENT_TASKS_PER_INSTANCE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      // Only generate tasks for OFFLINE tables
      String offlineTableName = tableConfig.getTableName();
      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating SegmentGenerationAndPushTask for non-OFFLINE table: {}", offlineTableName);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkNotNull(tableTaskConfig);
      Map<String, String> taskConfigs =
          tableTaskConfig.getConfigsForTaskType(STARTREE_MINION_TASK);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", offlineTableName);

      // Get max number of tasks for this table
      int tableMaxNumTasks;
      String tableMaxNumTasksConfig = taskConfigs.get(MinionConstants.TABLE_MAX_NUM_TASKS_KEY);
      if (tableMaxNumTasksConfig != null) {
        try {
          tableMaxNumTasks = Integer.parseInt(tableMaxNumTasksConfig);
        } catch (NumberFormatException e) {
          tableMaxNumTasks = Integer.MAX_VALUE;
        }
      } else {
        tableMaxNumTasks = Integer.MAX_VALUE;
      }

      // Generate tasks
      int tableNumTasks = 0;
      // Generate up to tableMaxNumTasks tasks each time for each table
      if (tableNumTasks == tableMaxNumTasks) {
        break;
      }
      String batchSegmentIngestionType = IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig);
      String batchSegmentIngestionFrequency = IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig);
      BatchIngestionConfig batchIngestionConfig = tableConfig.getIngestionConfig().getBatchIngestionConfig();
      List<Map<String, String>> batchConfigMaps = batchIngestionConfig.getBatchConfigMaps();
      for (Map<String, String> batchConfigMap : batchConfigMaps) {
        // TODO filter to only use sql connector batch configs
        try {
          List<OfflineSegmentZKMetadata> offlineSegmentsMetadata = Collections.emptyList();

          /*
          LOGIC for determining if task is already in progress or segments that have already been pushed

           if (BatchConfigProperties.SegmentIngestionType.APPEND.name().equalsIgnoreCase(batchSegmentIngestionType)) {
            offlineSegmentsMetadata = this._clusterInfoAccessor.getOfflineSegmentsMetadata(offlineTableName);
          }
          Set<String> existingSegmentInputFiles = getExistingSegmentInputFiles(offlineSegmentsMetadata);
          Set<String> inputFilesFromRunningTasks = getInputFilesFromRunningTasks();
          existingSegmentInputFiles.addAll(inputFilesFromRunningTasks);
          LOGGER.info("Trying to extract input files from path: {}, "
                  + "and exclude input files from existing segments metadata: {}, "
                  + "and input files from running tasks: {}", inputDirURI, existingSegmentInputFiles,
              inputFilesFromRunningTasks);
           */
          // For append mode, we don't create segments for input file URIs already created.

          //List<URI> inputFileURIs = getInputFilesFromDirectory(batchConfigMap, inputDirURI, existingSegmentInputFiles);

          //HARDCODE for testing
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
          // end of hard code

          List<String> smallQueryChunks = getSmallQueryChunks(sqlQueryConfig, sqlConnectorConfig);
          LOGGER.info("Chunk queries for task config generation: {}", smallQueryChunks);
          for (String smallQueryChunk : smallQueryChunks) {
            Map<String, String> singleFileGenerationTaskConfig =
                getSingleFileGenerationTaskConfig(offlineTableName, tableNumTasks, smallQueryChunk, batchConfigMap);
            pinotTaskConfigs.add(new PinotTaskConfig(STARTREE_MINION_TASK, singleFileGenerationTaskConfig));
            tableNumTasks++;

            // Generate up to tableMaxNumTasks tasks each time for each table
            if (tableNumTasks == tableMaxNumTasks) {
              break;
            }
          }
        } catch (Exception e) {
          LOGGER.error("Unable to generate the SegmentGenerationAndPush task. [ table configs: {}, task configs: {} ]",
              tableConfig, taskConfigs, e);
        }
      }
    }
    return pinotTaskConfigs;
  }

  private LocalDateTime getNextDataPullDateTime(LocalDateTime dateTime, long pullAmount, String pullGranularity) {
    switch (pullGranularity) {
      case "NANOS":
        return dateTime.plusNanos(pullAmount);
      case "SECONDS":
        return dateTime.plusSeconds(pullAmount);
      case "MINUTES":
        return dateTime.plusMinutes(pullAmount);
      case "HOURS":
        return dateTime.plusHours(pullAmount);
      case "DAYS":
        return dateTime.plusDays(pullAmount);
      case "WEEKS":
        return dateTime.plusWeeks(pullAmount);
      case "MONTHS":
        return dateTime.plusMonths(pullAmount);
      case "YEARS":
        return dateTime.plusYears(pullAmount);
      default:
        throw new UnsupportedOperationException("Data pull granularity not support: " + pullGranularity);
    }
  }

  private LocalDateTime getNextBatchEnd(LocalDateTime dateTime, LocalDateTime endDateTime, long pullAmount, String pullGranularity) {
    LocalDateTime nextDateTime = getNextDataPullDateTime(dateTime, pullAmount, pullGranularity);
    if (nextDateTime.isAfter(endDateTime)) {
      nextDateTime = endDateTime;
    }
    return nextDateTime;
  }

  private List<String> getSmallQueryChunks(SqlQueryConfig queryConfig, SqlConnectorConfig sqlConnectorConfig) throws Exception {
    LocalDateTime windowStart = queryConfig.getStartDateTime();

    if (queryConfig.getBatchQueryConfig() == null) {
      // Determine batch sizes using the default number of rows per file
      long numBatches = (long) Math.ceil((double) getNumberOfRows(sqlConnectorConfig, windowStart, queryConfig.getEndDateTime(), queryConfig.getQueryTemplate(), queryConfig.getTimeColumnName(), queryConfig.getTimeColumnFormat()) / DEFAULT_BATCH_NUM_ROWS);

      long batchPullAmount = Duration.between(queryConfig.getStartDateTime(), queryConfig.getEndDateTime()).toNanos() / numBatches;

      queryConfig.setBatchQueryConfig(new SqlQueryConfig.BatchQueryConfig(batchPullAmount, "NANOS")); //Use constant
    }


    SqlNode sqlNode = SqlParser.create(queryConfig.getQueryTemplate(), SqlParser.config()).parseQuery();
    if (!sqlNode.isA(Set.of(SqlKind.SELECT))) {
      throw new IllegalArgumentException("Invalid query. Must provide a SELECT sql statement");
    }

    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    SqlBasicCall dateRangeNode = findBetweenOperator(sqlSelect.getWhere(), queryConfig.getTimeColumnName());
    SqlIdentifier timeColumn = (SqlIdentifier) dateRangeNode.getOperands()[0];

    LocalDateTime batchStart = windowStart;
    LocalDateTime batchEnd;
    boolean isLastChunk = false;
    List<String> chunkQueries = new ArrayList<>();

    Function<LocalDateTime, String> dateTimeToDBFormatConverter = getDateTimeToDatabaseFormatConverter(
        queryConfig.getTimeColumnFormat());

    do {
      batchEnd = getNextBatchEnd(batchStart, queryConfig.getEndDateTime(),
          queryConfig.getBatchQueryConfig().getPullAmount(),
          queryConfig.getBatchQueryConfig().getPullGranularity()
      );

      if (batchEnd == queryConfig.getEndDateTime()) {
        isLastChunk = true;
      }

      //TODO: can make dynamic. if we can pull more rows if we need to. make another count(*) recursively.
      String chunkQuery = replaceQueryStartAndEndBatch(
          sqlSelect,
          dateRangeNode,
          timeColumn,
          dateTimeToDBFormatConverter.apply(batchStart),
          dateTimeToDBFormatConverter.apply(batchEnd),
          isLastChunk);
      chunkQueries.add(chunkQuery);
      batchStart = batchEnd;
    } while (!isLastChunk);

    return chunkQueries;
  }

  /**
   * Move to db query util
   * @param sqlConnectorConfig
   * @param dateTimeStart
   * @param dateTimeEnd
   * @return
   * @throws Exception
   */
  private long getNumberOfRows(SqlConnectorConfig sqlConnectorConfig, LocalDateTime dateTimeStart, LocalDateTime dateTimeEnd, String sqlQueryTemplate, String timeColumnName, String timeColumnFormat)
      throws Exception {
    // Generate count(*) query to get number of rows)

    //TODO fix this. do not store into properties object anymore
    Statement statement = getJDBCConnection(
        sqlConnectorConfig.getConnectProperties().getProperty("user"),
        sqlConnectorConfig.getConnectProperties().getProperty("password"),
        sqlConnectorConfig.getConnectProperties().getProperty("account"),
        sqlConnectorConfig.getConnectProperties().getProperty("db"),
        sqlConnectorConfig.getConnectProperties().getProperty("schema")
    );

    SqlNode sqlNode = SqlParser.create(sqlQueryTemplate, SqlParser.config()).parseQuery();

    if (!sqlNode.isA(Set.of(SqlKind.SELECT))) {
      throw new IllegalArgumentException("Invalid query. Must provide a SELECT sql statement");
    }

    SqlSelect sqlSelect = (SqlSelect) sqlNode;
    SqlBasicCall dateRangeNode = findBetweenOperator(sqlSelect.getWhere(), timeColumnName);

    //Set start and end identifiers
    ((SqlIdentifier) dateRangeNode.getOperands()[1])
        .setNames(Collections.singletonList("'" + convertDateTimeToDatabaseFormat(dateTimeStart, timeColumnFormat) + "'"), null);
    ((SqlIdentifier) dateRangeNode.getOperands()[2])
        .setNames(Collections.singletonList("'" + convertDateTimeToDatabaseFormat(dateTimeEnd, timeColumnFormat) + "'"), null);


    // Just choose first column in select which is used to make a SELECT COUNT(XXX) query
    SqlNode selectNodeFirst = sqlSelect.getSelectList().get(0);
    SqlCall countCall = SqlStdOperatorTable.COUNT.createCall(SqlParserPos.ZERO, selectNodeFirst);
    sqlSelect.setSelectList(SqlNodeList.of(countCall));

    String countQueryWithTime = replaceQueryStartAndEnd(sqlSelect,
        dateRangeNode,
        convertDateTimeToDatabaseFormat(dateTimeStart, timeColumnFormat),
        convertDateTimeToDatabaseFormat(dateTimeEnd, timeColumnFormat)
    );

    LOGGER.info("Making count query {}", countQueryWithTime);
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

  private String convertDateTimeToDatabaseFormat(LocalDateTime dateTime, String timeColumnFormat) {
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

  private String replaceQueryStartAndEndBatch(SqlSelect sqlSelect, SqlBasicCall dateRangeNode, SqlIdentifier timeColumn, String startReplace,
      String endReplace, boolean isLastChunk) {

    SqlNode[] startCalls = new SqlNode[2];
    SqlLiteral startTime = SqlLiteral.createDate(new DateString(startReplace), SqlParserPos.ZERO);
    startCalls[0] = timeColumn;
    startCalls[1] = startTime;
    SqlBasicCall gtStart = new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, startCalls, SqlParserPos.ZERO);

    SqlNode[] endCalls = new SqlNode[2];
    SqlLiteral endTime = SqlLiteral.createDate(new DateString(endReplace), SqlParserPos.ZERO);
    endCalls[0] = timeColumn;
    endCalls[1] = endTime;

    SqlOperator ltOperator = isLastChunk ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;

    SqlBasicCall ltEnd = new SqlBasicCall(ltOperator, endCalls, SqlParserPos.ZERO);

    dateRangeNode.setOperator(SqlStdOperatorTable.AND);
    dateRangeNode.setOperand(0, gtStart); //set start
    dateRangeNode.setOperand(1, ltEnd); //set end

    String chunkQuery = sqlSelect.toSqlString(new SnowflakeSqlDialect(SnowflakeSqlDialect.EMPTY_CONTEXT)).getSql();
    LOGGER.info("New query with replaced dates is {}", chunkQuery);
    return chunkQuery;
  }

  private Function<LocalDateTime, String> getDateTimeToDatabaseFormatConverter(String timeColumnFormat) {
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

  /**
   * Put in SQL utils
   * @param sqlNode
   * @return
   */
  private SqlBasicCall findBetweenOperator(SqlNode sqlNode, String timeColumnName) {
    if (sqlNode.getKind() == SqlKind.BETWEEN) {
      SqlBasicCall betweenCall = (SqlBasicCall) sqlNode;
      SqlNode columnName = betweenCall.getOperands()[0];
      SqlNode left = betweenCall.getOperands()[1];
      SqlNode right = betweenCall.getOperands()[2];

      if (columnName.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) columnName).names.get(0).equals(timeColumnName)
          && left.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) left).names.get(0).equals(SqlQueryConfig.START)
          && right.getKind() == SqlKind.IDENTIFIER
          && ((SqlIdentifier) right).names.get(0).equals(SqlQueryConfig.END)) {
        return betweenCall;
      }
    }

    if (sqlNode instanceof SqlBasicCall) {
      for (SqlNode node : ((SqlBasicCall) sqlNode).getOperandList()) {
        return findBetweenOperator(node, timeColumnName);
      }
    }

    throw new IllegalArgumentException("No between operator found!");
  }

  private Set<String> getInputFilesFromRunningTasks() {
    Set<String> inputFilesFromRunningTasks = new HashSet<>();
    Map<String, TaskState> taskStates =
        _clusterInfoAccessor.getTaskStates(MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE);
    for (String taskName : taskStates.keySet()) {
      switch (taskStates.get(taskName)) {
        case FAILED:
        case ABORTED:
        case STOPPED:
        case COMPLETED:
          continue;
      }
      List<PinotTaskConfig> taskConfigs = _clusterInfoAccessor.getTaskConfigs(taskName);
      for (PinotTaskConfig taskConfig : taskConfigs) {
        if (MinionConstants.SegmentGenerationAndPushTask.TASK_TYPE.equalsIgnoreCase(taskConfig.getTaskType())) {
          String inputFileURI = taskConfig.getConfigs().get(BatchConfigProperties.INPUT_DATA_FILE_URI_KEY);
          if (inputFileURI != null) {
            inputFilesFromRunningTasks.add(inputFileURI);
          }
        }
      }
    }
    return inputFilesFromRunningTasks;
  }

  private Map<String, String> getSingleFileGenerationTaskConfig(String offlineTableName, int sequenceID, String sqlQuery, Map<String, String> batchConfigMap) {
    Map<String, String> singleFileGenerationTaskConfig = new HashMap<>(batchConfigMap);
    singleFileGenerationTaskConfig
        .put(BatchConfigProperties.TABLE_NAME, TableNameBuilder.OFFLINE.tableNameWithType(offlineTableName));
    singleFileGenerationTaskConfig.put(StartreeMinionConstants.TASK_QUERY, sqlQuery);
    singleFileGenerationTaskConfig.put(BatchConfigProperties.SEQUENCE_ID, String.valueOf(sequenceID));
    singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _clusterInfoAccessor.getVipUrl());

    return singleFileGenerationTaskConfig;
  }
}
