

package com.borjav.data.service;

import com.borjav.data.converter.BigQuerySchemaConverter;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.SimpleTable;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Loads requested Table Schema using provided {@link BigQueryTableLoadService} followed by schema
 * translation using {@link BigQuerySchemaConverter}.
 */
public final class BigQueryZetaSqlSchemaLoader {

  private final BigQueryTableLoadService bqTableLoader;

  public BigQueryZetaSqlSchemaLoader(BigQueryTableLoadService bqTableLoader) {
    this.bqTableLoader = bqTableLoader;
  }


  public ImmutableSet<SimpleTable> loadSchemas(String... tableNames) {
    return bqTableLoader.loadTables(tableNames).stream()
        .map(BigQuerySchemaConverter::convert)
        .collect(toImmutableSet());
  }


  public ImmutableSet<SimpleTable> loadSchemas(ImmutableSet<String> tableNames) {
    return loadSchemas(tableNames.asList().toArray(new String[0]));
  }
}
