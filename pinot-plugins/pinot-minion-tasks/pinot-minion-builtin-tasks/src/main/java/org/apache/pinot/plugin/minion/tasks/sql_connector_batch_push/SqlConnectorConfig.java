package org.apache.pinot.plugin.minion.tasks.sql_connector_batch_push;

import java.util.Properties;


public interface SqlConnectorConfig {

  Properties getConnectProperties();

  String getConnectString();
}
