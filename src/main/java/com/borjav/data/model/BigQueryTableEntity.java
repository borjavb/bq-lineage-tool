
package com.borjav.data.model;

import com.google.auto.value.AutoValue;

import java.io.Serializable;

/**
 * Value class to represent a BiQgQuery Table entity.
 */
@AutoValue
public abstract class BigQueryTableEntity implements Serializable {

  public abstract String getProjectId();

  public abstract String getDataset();

  public abstract String getTable();

  /**
   * Returns {@code true} if the table is a temporary table.
   * <p> It uses rule dataset name starts with '_' or the table name starts with '_' or 'anon'.
   */
  public final boolean isTempTable() {
    return getDataset().startsWith("_")
        || getTable().startsWith("_")
        || getTable().startsWith("anon");
  }

  public static Builder builder() {
    return new AutoValue_BigQueryTableEntity.Builder();
  }

  public static BigQueryTableEntity create(String projectId, String dataset, String table) {
    return builder()
        .setProjectId(projectId)
        .setDataset(dataset)
        .setTable(table)
        .build();
  }


  public String getLegacySqlName() {
    return String.format("%s:%s.%s", getProjectId(), getDataset(), getTable());
  }

  public String getStandSqlName() {
    return String.format("%s.%s.%s", getProjectId(), getDataset(), getTable());
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDataset(String dataset);

    public abstract Builder setTable(String table);

    public abstract BigQueryTableEntity build();
  }
}
