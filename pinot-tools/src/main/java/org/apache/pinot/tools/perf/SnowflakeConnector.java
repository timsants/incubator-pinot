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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.io.IOUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("FieldCanBeLocal")
public class SnowflakeConnector extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnector.class);


  @Option(name = "-snowflakeTable", required = true, metaVar = "<int>", usage = "Snowflake Table from which to import data")
  private String _snowflakeTable;
  @Option(name = "-query", required = true, metaVar = "<int>", usage = "Templatized SQL query for pulling from Snowflake table")
  private int _query =;
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
    return "Run queries from a query file in singleThread, multiThreads, targetQPS or increasingQPS mode. E.g.\n"
        + "  QueryRunner -mode singleThread -queryFile <queryFile> -numTimesToRunQueries 0 -numIntervalsToReportAndClearStatistics 5\n"
        + "  QueryRunner -mode multiThreads -queryFile <queryFile> -numThreads 10 -reportIntervalMs 1000\n"
        + "  QueryRunner -mode targetQPS -queryFile <queryFile> -startQPS 50\n"
        + "  QueryRunner -mode increasingQPS -queryFile <queryFile> -startQPS 50 -deltaQPS 10 -numIntervalsToIncreaseQPS 20\n";
  }

  @Override
  public boolean execute()
      throws Exception {
   // Implement validation here

    return true;
  }
}
