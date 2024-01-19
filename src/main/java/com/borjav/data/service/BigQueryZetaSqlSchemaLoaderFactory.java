
package com.borjav.data.service;

import com.google.common.flogger.GoogleLogger;

import java.util.concurrent.TimeUnit;

/**
 * Factory to build BigQuerySchemaLoaders by instantiating BigQuery service using the provided
 * Credentials.
 */
public final class BigQueryZetaSqlSchemaLoaderFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  private final BigQueryServiceFactory bigQueryServiceFactory;

  public BigQueryZetaSqlSchemaLoaderFactory(BigQueryServiceFactory bigQueryServiceFactory) {
    this.bigQueryServiceFactory = bigQueryServiceFactory;
  }

  public static BigQueryZetaSqlSchemaLoaderFactory usingServiceFactory(
      BigQueryServiceFactory bigQueryServiceFactory) {
    return new BigQueryZetaSqlSchemaLoaderFactory(bigQueryServiceFactory);
  }

  public BigQueryZetaSqlSchemaLoader newLoader() {
    try {
      return new BigQueryZetaSqlSchemaLoader(
          BigQueryTableLoadService
              .usingServiceFactory(bigQueryServiceFactory));
    } catch (RuntimeException exception) {
      logger.atWarning()
          .withCause(exception)
          .atMostEvery(10, TimeUnit.MINUTES)
          .log("unable to create Bigquery service.");

      return null;
    }
  }
}
