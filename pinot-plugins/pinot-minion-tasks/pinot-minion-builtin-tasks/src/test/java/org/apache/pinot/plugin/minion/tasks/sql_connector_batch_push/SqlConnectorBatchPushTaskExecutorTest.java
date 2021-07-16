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

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.common.utils.config.TableConfigUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.plugin.minion.tasks.realtime_to_offline_segments.RealtimeToOfflineSegmentsTaskExecutor;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the {@link SqlConnectorBatchPushTaskExecutor}
 */
public class SqlConnectorBatchPushTaskExecutorTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "RealtimeToOfflineSegmentTaskExecutorTest");
  private static final File ORIGINAL_SEGMENT_DIR = new File(TEMP_DIR, "originalSegment");
  private static final File WORKING_DIR = new File(TEMP_DIR, "workingDir");
  private static final int NUM_SEGMENTS = 10;
  private static final int NUM_ROWS = 5;
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String TABLE_NAME_WITH_PARTITIONING = "testTableWithPartitioning_OFFLINE";
  private static final String TABLE_NAME_WITH_SORTED_COL = "testTableWithSortedCol_OFFLINE";
  private static final String TABLE_NAME_EPOCH_HOURS = "testTableEpochHours_OFFLINE";
  private static final String TABLE_NAME_SDF = "testTableSDF_OFFLINE";
  private static final String D1 = "d1";
  private static final String M1 = "m1";
  private static final String T = "t";
  private static final String T_TRX = "t_trx";

  private List<File> _segmentIndexDirList;
  private List<File> _segmentIndexDirListEpochHours;
  private List<File> _segmentIndexDirListSDF;


  @AfterClass
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
