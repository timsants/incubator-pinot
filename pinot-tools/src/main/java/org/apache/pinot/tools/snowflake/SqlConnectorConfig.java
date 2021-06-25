package org.apache.pinot.tools.snowflake;

import java.util.Properties;

public interface SqlConnectorConfig {

  Properties getConnectProperties();

  String getConnectString();
}
