

package com.borjav.data.extractor;

import com.borjav.data.model.BigQueryTableEntity;
import com.google.common.collect.ImmutableList;
import com.google.common.flogger.GoogleLogger;
import com.borjav.data.options.Options;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A Factory to create BigQuery table entity by parsing different naming formats.
 */
public abstract class BigQueryTableCreator {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  /**
   * Matches the given String as a Legacy or Standard Table name. If no match found, it returns a
   *
   * @param bigQueryTableName the table name in legacy or standard SQL format.
   * @return Table information with valid values or if no match found then tableName set as the
   * input String or empty table name.
   */
  public static BigQueryTableEntity usingBestEffort(String bigQueryTableName) {
    if (bigQueryTableName != null && bigQueryTableName.startsWith("$")) {
      return BigQueryTableEntity.create(null, null, bigQueryTableName);
    }

    for (String pattern : ImmutableList
        .of(BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT, BQ_RESOURCE_FORMAT, BQ_LINKED_RESOURCE_FORMAT,
            BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT_NOT_FULLY_QUALIFIED)) {
      try {
        return extractInformation(pattern, bigQueryTableName);
      } catch (IllegalArgumentException aex) {
        logger.atInfo().atMostEvery(1, TimeUnit.MINUTES).withCause(aex)
            .log("error parsing %s", bigQueryTableName);
      }
    }

    throw new IllegalArgumentException(
        "Couldn't convert into any known types: (" + bigQueryTableName + ")");
  }

  /**
   * Returns a parsed TableEntity from the legacy SQL form (<project-id>:<dataset-id>.<table-id>) of
   * a BigQuery table.
   */
  public static BigQueryTableEntity fromLegacyTableName(String legacyName) {
    return extractInformation(LEGACY_TABLE_FORMAT, legacyName);
  }

  public static BigQueryTableEntity fromSqlResource(String sqlResource) {
    return extractInformation(SQL_RESOURCE_FORMAT, sqlResource);
  }

  public static BigQueryTableEntity fromBigQueryResource(String resource) {
    return extractInformation(BQ_RESOURCE_FORMAT, resource);
  }

  public static BigQueryTableEntity fromLinkedResource(String linkedResource) {
    return extractInformation(BQ_LINKED_RESOURCE_FORMAT, linkedResource);
  }

  private static final String PROJECT_ID_TAG = "projectId";

  private static final String DATASET_ID_TAG = "dataset";

  private static final String TABLE_ID_TAG = "table";

  private static final String PROJECT_PATTERN = "[a-zA-Z0-9\\.\\-\\:]+";

  private static final String DATASET_PATTERN = "[a-zA-Z_][a-zA-Z0-9\\_]+";

  private static final String TABLE_PATTERN = "[a-zA-Z0-9][a-zA-Z0-9\\_\\*]+";

  private static final String LEGACY_TABLE_FORMAT =
      String.format(
          "^(?<%s>%s)\\:(?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String SQL_RESOURCE_FORMAT =
      String.format(
          "^bigquery\\.table\\.(?<%s>%s)\\.(?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_RESOURCE_FORMAT =
      String.format(
          "^projects/(?<%s>%s)/datasets/(?<%s>%s)/tables/(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_LINKED_RESOURCE_FORMAT =
      String.format(
          "^//bigquery.googleapis.com/projects/(?<%s>%s)/datasets/(?<%s>%s)/tables/(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT =
      String.format(
          "^(?<%s>%s)[:\\.](?<%s>%s)\\.(?<%s>%s)$",
          PROJECT_ID_TAG, PROJECT_PATTERN, DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG,
          TABLE_PATTERN);

  private static final String BQ_LEGACY_STANDARD_TABLE_NAME_FORMAT_NOT_FULLY_QUALIFIED =
      String.format(
          "^(?<%s>%s)\\.(?<%s>%s)$",
          DATASET_ID_TAG, DATASET_PATTERN, TABLE_ID_TAG, TABLE_PATTERN);

  //TODO: How to access INFORMATION_SCHEMA

  private static BigQueryTableEntity extractInformation(String pattern, String resource) {
    Matcher matcher = Pattern.compile(pattern).matcher(resource);
    if (!matcher.find()) {
      throw new IllegalArgumentException(
          "input (" + resource + ") not in correct format (" + pattern + ")");
    }

    String projectID = Options.default_project;
    if (matcher.groupCount() > 2) {
      projectID = matcher.group(PROJECT_ID_TAG);
    } else {
      Options.missing_project.put(
          projectID + "." + matcher.group(DATASET_ID_TAG) + "." + matcher.group(TABLE_ID_TAG),
          matcher.group(DATASET_ID_TAG) + "." + matcher.group(TABLE_ID_TAG));
    }

    return BigQueryTableEntity.builder()
        .setProjectId(projectID)
        .setDataset(matcher.group(DATASET_ID_TAG))
        .setTable(matcher.group(TABLE_ID_TAG))
        .build();
  }

  private BigQueryTableCreator() {
  }
}
