package org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import javax.annotation.Nullable;


/**
 * TODO consider builder pattern?
 *
 * Query related parameters for pulling data in batches using {@link SqlConnector}
 */
public class SqlQueryConfig {

  //Templatized SQL query for pulling from Snowflake table
  private String _queryTemplate;
  private String _timeColumnFormat; //format of time column expressed as date format. other accepted values are millisecondsSinceEpoch and secondsSinceEpoch.
  private String _timeColumnName;

  private String _startEndDateTimeFormat = "yyyy-MM-dd"; //optional; Format of startTime and endTime
  private String _startTime; //string ISO format or could add format...
  private String _endTime;

  private LocalDateTime _startDateTime;
  private LocalDateTime _endDateTime;

  private BatchQueryConfig _batchQueryConfig;

  public static final String START = "$START";
  public static final String END = "$END";


  public SqlQueryConfig(String queryTemplate,
      String timeColumnFormat,
      String timeColumnName,
      String startEndDateTimeFormat,
      String startTime,
      String endTime,
      @Nullable BatchQueryConfig batchQueryConfig) {
    _queryTemplate = queryTemplate;
    _timeColumnFormat = timeColumnFormat;
    _timeColumnName = timeColumnName;
    _startEndDateTimeFormat = startEndDateTimeFormat;
    _startTime = startTime;
    _endTime = endTime;
    _batchQueryConfig = batchQueryConfig;

    validate();

    DateTimeFormatter windowFormatter = new DateTimeFormatterBuilder()
        .appendPattern(_startEndDateTimeFormat)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter();

    _startDateTime = LocalDateTime.parse(_startTime, windowFormatter);
    _endDateTime = LocalDateTime.parse(_endTime, windowFormatter);
  }

  public void setBatchQueryConfig(BatchQueryConfig batchQueryConfig) {
    _batchQueryConfig = batchQueryConfig;
  }

  private void validate() throws IllegalStateException {

    /*
    Preconditions.checkArgument(_queryTemplate.contains("> $START"),
        "Query template is missing '> $START' in WHERE clause");
    Preconditions.checkArgument(_queryTemplate.contains("< $END"),
        "Query template is missing '< $END' in WHERE clause");
*/

    //what if data pull amount is finer granularity than time format
  }

  public String getQueryTemplate() {
    return _queryTemplate;
  }

  public String getTimeColumnFormat() {
    return _timeColumnFormat;
  }

  public String getStartEndDateTimeFormat() {
    return _startEndDateTimeFormat;
  }

  public LocalDateTime getStartDateTime() {
    return _startDateTime;
  }

  public LocalDateTime getEndDateTime() {
    return _endDateTime;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public BatchQueryConfig getBatchQueryConfig() {
    return _batchQueryConfig;
  }

  public static class BatchQueryConfig {
    private long _pullAmount;
    private String _pullGranularity;

    public BatchQueryConfig(long pullAmount, String pullGranularity) {
      _pullAmount = pullAmount;
      _pullGranularity = pullGranularity;
    }

    public long getPullAmount() {
      return _pullAmount;
    }

    public String getPullGranularity() {
      return _pullGranularity;
    }
  }
}
