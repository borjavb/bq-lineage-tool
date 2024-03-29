
package com.borjav.data.service;

import com.borjav.data.exception.BigQueryOperationException;
import com.borjav.data.extractor.BigQueryTableCreator;
import com.borjav.data.model.BigQueryTableEntity;
import com.google.api.services.bigquery.model.Table;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.flogger.GoogleLogger;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Provides loading Table information using BigQuery API. It {@link Cache}s the reads from the API
 * for 5 minutes.
 */
public final class BigQueryTableLoadService implements Serializable {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final BigQueryServiceFactory bqServiceFactory;
  private static final Cache<BigQueryTableEntity, Table> LOCAL_CACHE = buildCache();

  public BigQueryTableLoadService(
      BigQueryServiceFactory bqServiceFactory) {
    this.bqServiceFactory = bqServiceFactory;
  }

  public static BigQueryTableLoadService usingServiceFactory(
      BigQueryServiceFactory bqServiceFactory) {
    return new BigQueryTableLoadService(bqServiceFactory);
  }

  private static Cache<BigQueryTableEntity, Table> buildCache() {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(Duration.ofMinutes(20))
        .maximumSize(10000L)
        .build();
  }

  /**
   * Loads a single table information.
   *
   * @param tableName the fully qualified table name.
   */
  public Table loadTable(String tableName) {
    return loadTable(BigQueryTableCreator.usingBestEffort(tableName));
  }

  /**
   * Loads a single table information.
   *
   * @param table the fully qualified table name.
   */
  public Table loadTable(BigQueryTableEntity table) {
    try {
      //check in cache
      return LOCAL_CACHE.get(table, () -> loadSingleTableFromServer(table));
    } catch (ExecutionException executionException) {
      logger.atWarning()
          .withCause(executionException)
          .atMostEvery(1, TimeUnit.MINUTES)
          .log(String.format("Unable to load table %s", table));

      throw new BigQueryOperationException(table, executionException);
    }
  }

  /**
   * Loads multiple tables' information.
   *
   * @param tableNames multiple fully qualified table names.
   */
  public ImmutableSet<Table> loadTables(String... tableNames) {
    return loadTables(
        Arrays.stream(tableNames)
            .distinct()
            .map(BigQueryTableCreator::usingBestEffort)
            .collect(toImmutableSet()));
  }

  /**
   * Loads multiple tables from BigQuery, with Cache-through.
   * <b>It does NOT use Batch API.</b>
   *
   * @param tables the list of table names to load
   * @return all the loaded tables. It can throw runtime exception if table is not found.
   */
  public ImmutableSet<Table> loadTables(ImmutableSet<BigQueryTableEntity> tables) {
    ImmutableMap<BigQueryTableEntity, Table> allCachedData =
        LOCAL_CACHE.getAllPresent(tables);
    ImmutableSet<BigQueryTableEntity> tablesToFetch
        = Sets.difference(tables, allCachedData.keySet()).immutableCopy();

    return Sets.union(ImmutableSet.copyOf(allCachedData.values()),
        bulkFetchFromServer(tablesToFetch)).immutableCopy();
  }

  private Table loadSingleTableFromServer(BigQueryTableEntity tableSpec) throws IOException {
    return bqServiceFactory.buildService()
        .tables()
        .get(tableSpec.getProjectId(), tableSpec.getDataset(), tableSpec.getTable())
        .execute();
  }

  private ImmutableSet<Table> bulkFetchFromServer(ImmutableSet<BigQueryTableEntity> tablesToFetch) {
    return tablesToFetch.stream()
        .map(this::loadTable)
        .collect(toImmutableSet());
  }

  @VisibleForTesting
  static void clearLocalCache() {
    LOCAL_CACHE.invalidateAll();
    LOCAL_CACHE.cleanUp();
  }
}
