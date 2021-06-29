package org.apache.pinot.tools.snowflake;

import com.google.common.base.Preconditions;


/**
 * TODO consider builder pattern?
 *
 * Query related parameters for pulling data in batches using {@link SqlConnector}
 */
public class SqlQueryConfig {

  //Templatized SQL query for pulling from Snowflake table
  private String _queryTemplate;
  private String _timeColumnFormat; //format of time column expressed as date format. other accepted values are millisecondsSinceEpoch and secondsSinceEpoch.
  private String _timeColumnName; //name of column

  private long _dataPullAmount; //? do we need this
  private String _dataPullGranularity; //how big each chunk should be SECONDS, MINUTES, HOURS, DAYS

  private String _windowDateTimeFormat = "yyyy-MM-dd"; //optional; Format of startTime and endTime
  private String _startTime; //string ISO format or could add format...
  private String _endTime;

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
  }

  private void validate() throws IllegalStateException {
    Preconditions.checkArgument(_queryTemplate.contains("> $START"),
        "Query template is missing '> $START' in WHERE clause");
    Preconditions.checkArgument(_queryTemplate.contains("< $END"),
        "Query template is missing '< $END' in WHERE clause");


    //what if data pull amount is finer granularity than time format
  }

  public String getQueryTemplate() {
    return _queryTemplate;
  }

  public String getTimeColumnFormat() {
    return _timeColumnFormat;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
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
}
