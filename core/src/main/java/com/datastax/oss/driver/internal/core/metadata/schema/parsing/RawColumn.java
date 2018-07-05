/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.metadata.schema.parsing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * An intermediary format to manipulate columns before we turn them into {@link ColumnMetadata}
 * instances.
 */
@NotThreadSafe
public class RawColumn implements Comparable<RawColumn> {

  public static List<RawColumn> toRawColumns(
      Collection<AdminRow> rows,
      CqlIdentifier keyspaceId,
      Map<CqlIdentifier, UserDefinedType> userTypes) {
    if (rows.isEmpty()) {
      return Collections.emptyList();
    } else {
      // Use a mutable list, we might remove some elements later
      List<RawColumn> result = Lists.newArrayListWithExpectedSize(rows.size());
      for (AdminRow row : rows) {
        result.add(new RawColumn(row, keyspaceId, userTypes));
      }
      return result;
    }
  }

  public static final String KIND_PARTITION_KEY = "partition_key";
  public static final String KIND_CLUSTERING_COLUMN = "clustering";
  public static final String KIND_REGULAR = "regular";
  public static final String KIND_COMPACT_VALUE = "compact_value";
  public static final String KIND_STATIC = "static";

  public final CqlIdentifier name;
  public String kind;
  public final int position;
  public final String dataType;
  public final boolean reversed;
  public final String indexName;
  public final String indexType;
  public final Map<String, String> indexOptions;

  private RawColumn(
      AdminRow row, CqlIdentifier keyspaceId, Map<CqlIdentifier, UserDefinedType> userTypes) {
    // Cassandra < 3.0:
    // CREATE TABLE system.schema_columns (
    //     keyspace_name text,
    //     columnfamily_name text,
    //     column_name text,
    //     component_index int,
    //     index_name text,
    //     index_options text,
    //     index_type text,
    //     type text,
    //     validator text,
    //     PRIMARY KEY (keyspace_name, columnfamily_name, column_name)
    // ) WITH CLUSTERING ORDER BY (columnfamily_name ASC, column_name ASC)
    //
    // Cassandra >= 3.0:
    // CREATE TABLE system_schema.columns (
    //     keyspace_name text,
    //     table_name text,
    //     column_name text,
    //     clustering_order text,
    //     column_name_bytes blob,
    //     kind text,
    //     position int,
    //     type text,
    //     PRIMARY KEY (keyspace_name, table_name, column_name)
    // ) WITH CLUSTERING ORDER BY (table_name ASC, column_name ASC)
    this.name = CqlIdentifier.fromInternal(row.getString("column_name"));
    if (row.contains("kind")) {
      this.kind = row.getString("kind");
    } else {
      this.kind = row.getString("type");
      // remap clustering_key to KIND_CLUSTERING_COLUMN so code doesn't have to check for both.
      if (this.kind.equals("clustering_key")) {
        this.kind = KIND_CLUSTERING_COLUMN;
      }
    }

    Integer rawPosition =
        row.contains("position") ? row.getInteger("position") : row.getInteger("component_index");
    this.position = (rawPosition == null || rawPosition == -1) ? 0 : rawPosition;

    this.dataType = row.contains("validator") ? row.getString("validator") : row.getString("type");
    this.reversed =
        row.contains("clustering_order")
            ? "desc".equals(row.getString("clustering_order"))
            : DataTypeClassNameParser.isReversed(dataType);
    this.indexName = row.getString("index_name");
    this.indexType = row.getString("index_type");
    // index_options can apparently contain the string 'null' (JAVA-834)
    String indexOptionsString = row.getString("index_options");
    this.indexOptions =
        (indexOptionsString == null || indexOptionsString.equals("null"))
            ? Collections.emptyMap()
            : SimpleJsonParser.parseStringMap(indexOptionsString);
  }

  @Override
  public int compareTo(@NonNull RawColumn that) {
    // First, order by kind. Then order partition key and clustering columns by position. For
    // other kinds, order by column name.
    if (!this.kind.equals(that.kind)) {
      return this.kind.compareTo(that.kind);
    } else if (kind.equals(KIND_PARTITION_KEY) || kind.equals(KIND_CLUSTERING_COLUMN)) {
      return Integer.compare(this.position, that.position);
    } else {
      return this.name.asInternal().compareTo(that.name.asInternal());
    }
  }
}
