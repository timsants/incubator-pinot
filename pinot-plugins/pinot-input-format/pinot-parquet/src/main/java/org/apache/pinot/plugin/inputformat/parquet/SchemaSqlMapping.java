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

import org.apache.avro.Schema;

/**
 * Mapping between an sql column and schema.
 */
public class SchemaSqlMapping {

  private final String schemaName, sqlColumnName;

  private final int sqlType;
  private final Schema.Type schemaType;

  /**
   *
   * @param schemaName
   * @param sqlColumnName
   * @param sqlType {@link java.sql.Types }
   * @param schemaType
   */
  public SchemaSqlMapping(String schemaName, String sqlColumnName, int sqlType, Schema.Type schemaType) {
    this.schemaName = schemaName;
    this.sqlColumnName = sqlColumnName;
    this.sqlType = sqlType;
    this.schemaType = schemaType;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getSqlColumnName() {
    return sqlColumnName;
  }

  public int getSqlType() {
    return sqlType;
  }

  public Schema.Type getSchemaType() {
    return schemaType;
  }
}
