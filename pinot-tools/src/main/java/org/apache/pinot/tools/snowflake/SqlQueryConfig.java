package org.apache.pinot.tools.snowflake;

import com.google.common.base.Preconditions;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;


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
  private long _dataPullAmount; //? do we need this
  private String _dataPullGranularity; //how big each chunk should be SECONDS, MINUTES, HOURS, DAYS

  private String _windowDateTimeFormat = "yyyy-MM-dd"; //optional; Format of startTime and endTime
  private String _startTime; //string ISO format or could add format...
  private String _endTime;

  private LocalDateTime _windowStartTime;
  private LocalDateTime _windowEndTime;

  public static final String START = "$START";
  public static final String END = "$END";


  public SqlQueryConfig(String queryTemplate, String timeColumnFormat, String timeColumnName, long dataPullAmount,
      String dataPullGranularity, String windowDateTimeFormat, String startTime, String endTime) {
    _queryTemplate = queryTemplate;
    _timeColumnFormat = timeColumnFormat;
    _timeColumnName = timeColumnName;
    _dataPullAmount = dataPullAmount;
    _dataPullGranularity = dataPullGranularity;
    _windowDateTimeFormat = windowDateTimeFormat;
    _startTime = startTime;
    _endTime = endTime;


    validate();

    DateTimeFormatter windowFormatter = new DateTimeFormatterBuilder()
        .appendPattern(_windowDateTimeFormat)
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .toFormatter();

    _windowStartTime = LocalDateTime.parse(_startTime, windowFormatter);
    _windowEndTime = LocalDateTime.parse(_endTime, windowFormatter);
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

  public long getDataPullAmount() {
    return _dataPullAmount;
  }

  public String getWindowDateTimeFormat() {
    return _windowDateTimeFormat;
  }

  public String getStartTime() {
    return _startTime;
  }

  public String getEndTime() {
    return _endTime;
  }

  public String getDataPullGranularity() {
    return _dataPullGranularity;
  }

  public LocalDateTime getWindowStartTime() {
    return _windowStartTime;
  }

  public LocalDateTime getWindowEndTime() {
    return _windowEndTime;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }
}
