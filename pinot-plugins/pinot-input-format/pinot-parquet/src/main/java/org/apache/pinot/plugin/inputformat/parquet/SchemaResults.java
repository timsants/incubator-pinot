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

import java.util.List;
import org.apache.avro.Schema;

/**
 * Contains both the {@link org.apache.avro.Schema } and the mapping between the sql column name and type,
 * and associated schema type.
 */
public class SchemaResults {

  private Schema parsedSchema;

  private List<SchemaSqlMapping> mappings;

  public Schema getParsedSchema() {
    return parsedSchema;
  }

  public void setParsedSchema(Schema parsedSchema) {
    this.parsedSchema = parsedSchema;
  }

  public List<SchemaSqlMapping> getMappings() {
    return mappings;
  }

  public void setMappings(List<SchemaSqlMapping> mappings) {
    this.mappings = mappings;
  }
}