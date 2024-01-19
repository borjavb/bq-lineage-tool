

package com.borjav.data.converter;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.StructType;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.borjav.data.model.BigQueryTableEntity;
import java.util.ArrayList;
import java.util.List;

/**
 * An adaptor to convert BigQuery Table schema to ZetaSQL Table schema.
 */
public final class BigQuerySchemaConverter {

  /**
   * Converts a BigQuery {@link TableSchema} to ZetaSql Table schema.
   *
   * @param bigQueryTable the BigQuery Table as returned from BigQuery API.
   * @return a ZetaSql table reflecting the same name and schema as the input.
   */
  public static SimpleTable convert(Table bigQueryTable) {

    if (!bigQueryTable.getType().equals("TABLE") && !bigQueryTable.getType().equals("EXTERNAL")
        && !bigQueryTable.getType().equals("VIEW")) {
      throw new IllegalArgumentException(
          "Table Type should be \"TABLE\" or \"EXTERNAL\" found \"" + bigQueryTable.getType()
          + "\"");
    }
    BigQueryTableEntity tableSpec = extractTableSpec(bigQueryTable.getTableReference());
    return
        new SimpleTable(
            tableSpec.getStandSqlName(),
            extractSchema(tableSpec.getStandSqlName(), bigQueryTable.getSchema()));
  }

  private static BigQueryTableEntity extractTableSpec(TableReference bqTableRef) {
    return
        BigQueryTableEntity.builder()
            .setProjectId(bqTableRef.getProjectId())
            .setDataset(bqTableRef.getDatasetId())
            .setTable(bqTableRef.getTableId())
            .build();
  }

  private static ImmutableList<SimpleColumn> extractSchema(
      String tableName,
      TableSchema bqTableSchema) {
    ImmutableList<SimpleColumn> simpleSchema = bqTableSchema.getFields().stream()
        .map(field -> new SimpleColumn(tableName, field.getName(), extractColumnType(field)))
        .collect(toImmutableList());
    return ImmutableList.<SimpleColumn>builder()
        .addAll(simpleSchema)
        .addAll(buildPseudoColumns(tableName))
        .build();


  }


  private static ImmutableList<SimpleColumn> buildPseudoColumns(String tableName) {
  // adding default columns from BQ to the schemas
    List<SimpleColumn> pseudoColumns = new ArrayList<>() {
      {
        add(new SimpleColumn(
            tableName,
            "_PARTITIONTIME",
            TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP),
            true,
            false));//
        add(new SimpleColumn(
            tableName,
            "_PARTTIONDATE",
            TypeFactory.createSimpleType(TypeKind.TYPE_DATE),
            true,
            false));
        add(new SimpleColumn(
            tableName,
            "_TABLE_SUFFIX",
            TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
            true,
            false));
      }
    };

    return pseudoColumns.stream().collect(toImmutableList());
  }


  private static Type extractColumnType(TableFieldSchema fieldSchema) {
    Type fieldType;

    if ("RECORD".equals(fieldSchema.getType())) {

      ImmutableSet.Builder<StructType.StructField> fieldBuilder = ImmutableSet.builder();

      for (TableFieldSchema recordField : fieldSchema.getFields()) {
        Type recordFieldType = extractColumnType(recordField);

        fieldBuilder.add(new StructType.StructField(recordField.getName(), recordFieldType));
      }

      fieldType = TypeFactory.createStructType(fieldBuilder.build());
    } else {
      fieldType = TypeFactory.createSimpleType(convertSimpleType(fieldSchema.getType()));
    }

    if ("REPEATED".equals(fieldSchema.getMode())) {
      return TypeFactory.createArrayType(fieldType);
    }

    return fieldType;
  }

  private static TypeKind convertSimpleType(String bqType) {
    switch (bqType) {
      case "STRING":
        return TypeKind.TYPE_STRING;
      case "BYTES":
        return TypeKind.TYPE_BYTES;
      case "INTEGER":
        return TypeKind.TYPE_INT64;
      case "FLOAT":
        return TypeKind.TYPE_FLOAT;
      case "NUMERIC":
        return TypeKind.TYPE_NUMERIC;
      case "BOOLEAN":
        return TypeKind.TYPE_BOOL;
      case "TIMESTAMP":
        return TypeKind.TYPE_TIMESTAMP;
      case "DATE":
        return TypeKind.TYPE_DATE;
      case "TIME":
        return TypeKind.TYPE_TIME;
      case "DATETIME":
        return TypeKind.TYPE_DATETIME;
      case "GEOGRAPHY":
        return TypeKind.TYPE_GEOGRAPHY;
      case "BIGNUMERIC":
        return TypeKind.TYPE_BIGNUMERIC;
      case "INTERVAL":
        return TypeKind.TYPE_INTERVAL;
      case "JSON":
        return TypeKind.TYPE_JSON;
      default:
        return TypeKind.TYPE_UNKNOWN;
    }
  }

  private BigQuerySchemaConverter() {
  }
}
