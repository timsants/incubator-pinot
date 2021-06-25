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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 *
 */
public class ResultSetParquetTransformer {

  public InputStream transform(ResultSet resultSet, String schemaName, String namespace, java.nio.file.Path tempDir, String chunkNum) throws IOException, SQLException {

    SchemaResults schemaResults = new ResultSetSchemaGenerator().generateSchema(resultSet,
        schemaName, namespace);


    java.nio.file.Path tempFile = Files.createTempFile(tempDir, "chunk_" + chunkNum, ".parquet");

    Path outputPath = new Path(tempFile.toUri());

    final LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());

    File file = localFileSystem.pathToFile(outputPath);
    file.delete();

    ParquetWriter parquetWriter = AvroParquetWriter.builder(outputPath)
        .withSchema(schemaResults.getParsedSchema())
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();

    List<GenericRecord> records = new ArrayList<>();

    while (resultSet.next()) {

      GenericRecordBuilder builder = new GenericRecordBuilder(schemaResults.getParsedSchema());

      for (SchemaSqlMapping mapping : schemaResults.getMappings()) {

        builder.set(
            schemaResults.getParsedSchema().getField(mapping.getSchemaName()),
            extractResult(mapping, resultSet));
      }

      GenericRecord record = builder.build();

      records.add(record);
    }

    for (GenericRecord record : records) {
      parquetWriter.write(record);
    }

    parquetWriter.close();

    File outputFile = localFileSystem.pathToFile(outputPath);

    return new FileInputStream(outputFile);

  }

  /**
   * Extracts the appropriate value from the ResultSet using the given SchemaSqlMapping.
   *
   * @param mapping
   * @param resultSet
   * @return
   * @throws SQLException
   */
  static Object extractResult(SchemaSqlMapping mapping, ResultSet resultSet) throws SQLException {

    switch (mapping.getSqlType()) {
      case Types.BOOLEAN:
        return resultSet.getBoolean(mapping.getSqlColumnName());
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.ROWID:
        return resultSet.getInt(mapping.getSqlColumnName());
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.SQLXML:
      case Types.DATE:
        return resultSet.getString(mapping.getSqlColumnName());
      case Types.REAL:
      case Types.FLOAT:
        return resultSet.getFloat(mapping.getSqlColumnName());
      case Types.DOUBLE:
        return resultSet.getDouble(mapping.getSqlColumnName());
      case Types.NUMERIC:
        return resultSet.getBigDecimal(mapping.getSqlColumnName());
      case Types.DECIMAL:
        return resultSet.getBigDecimal(mapping.getSqlColumnName());
      case Types.TIME:
      case Types.TIME_WITH_TIMEZONE:
        return resultSet.getTime(mapping.getSqlColumnName()).getTime();
      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return resultSet.getTimestamp(mapping.getSqlColumnName()).getTime();
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.NULL:
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.DISTINCT:
      case Types.STRUCT:
      case Types.ARRAY:
      case Types.BLOB:
      case Types.CLOB:
      case Types.REF:
      case Types.DATALINK:
      case Types.NCLOB:
      case Types.REF_CURSOR:
        return resultSet.getByte(mapping.getSqlColumnName());
      default:
        return resultSet.getString(mapping.getSqlColumnName());
    }
  }
}
