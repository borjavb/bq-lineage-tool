/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package converter;

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.borjav.data.converter.BigQuerySchemaConverter;
import com.google.api.services.bigquery.model.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.zetasql.FileDescriptorSetsBuilder;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.StructType;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import utils.GoogleTypesToJsonConverter;
import utils.TestResourceLoader;

@RunWith(JUnit4.class)
public final class BigQuerySchemaConverterTest {

  private static ImmutableList<?> convertToSerializedForm(SimpleColumn... columns) {
    return Arrays.stream(columns)
        .map(col -> col.serialize(new FileDescriptorSetsBuilder()))
        .collect(toImmutableList());
  }

  private static ImmutableList<?> convertToSerializedForm(Collection<SimpleColumn> columns) {
    return convertToSerializedForm(columns.toArray(new SimpleColumn[0]));
  }

  @Test
  public void convert_simpleTypes_valid() {
    SimpleTable parsedTable =
        BigQuerySchemaConverter
            .convert(
                Objects.requireNonNull(GoogleTypesToJsonConverter.convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/bigquery_simple_type_table_schema.json"))));

    String expectedTableName = "myproject.dataset.table";

    MatcherAssert.assertThat(parsedTable.getName(), Is.is(expectedTableName));
    MatcherAssert.assertThat(convertToSerializedForm(parsedTable.getColumnList()),
        Is.is(
            convertToSerializedForm(
                new SimpleColumn(
                    expectedTableName, "afloat", TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
                new SimpleColumn(
                    expectedTableName,
                    "aString",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                new SimpleColumn(
                    expectedTableName,
                    "aInteger",
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    expectedTableName, "aBool", TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
                new SimpleColumn(
                    expectedTableName,
                    "_PARTITIONTIME",
                    TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP),
                    true,
                    false),
                new SimpleColumn(
                    expectedTableName,
                    "_PARTTIONDATE",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATE),
                    true,
                    false),
                new SimpleColumn(
                    expectedTableName,
                    "_TABLE_SUFFIX",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
                    true,
                    false)
            )));
  }

  @Test
  public void convert_allSimpleDataTypes_valid() {
    SimpleTable parsedTable =
        BigQuerySchemaConverter
            .convert(
                Objects.requireNonNull(GoogleTypesToJsonConverter.convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/bigquery_simple_all_types_table_schema.json"))));

    String expectedTableName = "myproject.dataset.table";
    MatcherAssert.assertThat(parsedTable.getName(), Is.is(expectedTableName));
    MatcherAssert.assertThat(convertToSerializedForm(parsedTable.getColumnList()),
        Is.is(
            convertToSerializedForm(
                new SimpleColumn(
                    expectedTableName,
                    "afloat",
                    TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
                new SimpleColumn(
                    expectedTableName,
                    "aString",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
                new SimpleColumn(
                    expectedTableName,
                    "aInteger",
                    TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
                new SimpleColumn(
                    expectedTableName,
                    "aBool",
                    TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
                new SimpleColumn(
                    expectedTableName,
                    "aBytes",
                    TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)),
                new SimpleColumn(
                    expectedTableName,
                    "aNumeric",
                    TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)),
                new SimpleColumn(
                    expectedTableName,
                    "aTimestamp",
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
                new SimpleColumn(
                    expectedTableName,
                    "aDate",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATE)),
                new SimpleColumn(
                    expectedTableName,
                    "aTime",
                    TypeFactory.createSimpleType(TypeKind.TYPE_TIME)),
                new SimpleColumn(
                    expectedTableName,
                    "aDateTime",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME)),
                new SimpleColumn(
                    expectedTableName,
                    "aGeoPoint",
                    TypeFactory.createSimpleType(TypeKind.TYPE_GEOGRAPHY)),
                new SimpleColumn(
                    expectedTableName,
                    "_PARTITIONTIME",
                    TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP),
                    true,
                    false),
                new SimpleColumn(
                    expectedTableName,
                    "_PARTTIONDATE",
                    TypeFactory.createSimpleType(TypeKind.TYPE_DATE),
                    true,
                    false),
                new SimpleColumn(
                    expectedTableName,
                    "_TABLE_SUFFIX",
                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
                    true,
                    false)

            )));
  }

  @Test
  public void convert_complexTable_valid() {
    SimpleTable parseTableSchema =
        BigQuerySchemaConverter
            .convert(
                Objects.requireNonNull(GoogleTypesToJsonConverter.convertFromJson(
                    Table.class,
                    TestResourceLoader
                        .load("schemas/simple_daily_report_table_schema.json"))));

    String expectedTableName = "myproject.reporting.daily_report";
    MatcherAssert.assertThat(parseTableSchema.getName(), Is.is(expectedTableName));
    MatcherAssert.assertThat(convertToSerializedForm(parseTableSchema.getColumnList()),
        Is.is(convertToSerializedForm(
            new SimpleColumn(
                expectedTableName,
                "hit_timestamp",
                TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
            new SimpleColumn(
                expectedTableName,
                "partner_id",
                TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
            new SimpleColumn(
                expectedTableName,
                "partner_name",
                TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
            new SimpleColumn(
                expectedTableName,
                "products",
                TypeFactory.createArrayType(
                    TypeFactory.createStructType(
                        ImmutableSet.<StructType.StructField>builder()
                            .add(
                                new StructType.StructField(
                                    "jsonError",
                                    TypeFactory.createSimpleType(TypeKind.TYPE_STRING)))
                            .add(
                                new StructType.StructField(
                                    "product",
                                    TypeFactory.createStructType(
                                        ImmutableSet.<StructType.StructField>builder()
                                            .add(
                                                new StructType.StructField(
                                                    "id",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .add(
                                                new StructType.StructField(
                                                    "name",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .add(
                                                new StructType.StructField(
                                                    "seller",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .add(
                                                new StructType.StructField(
                                                    "quantity",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .add(
                                                new StructType.StructField(
                                                    "value",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .add(
                                                new StructType.StructField(
                                                    "currency",
                                                    TypeFactory.createSimpleType(
                                                        TypeKind.TYPE_STRING)))
                                            .build())))
                            .build()))),
            new SimpleColumn(
                expectedTableName,
                "latency",
                TypeFactory.createSimpleType(TypeKind.TYPE_FLOAT)),
            new SimpleColumn(
                expectedTableName, "is_ok",
                TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
            new SimpleColumn(
                expectedTableName,
                "_PARTITIONTIME",
                TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP),
                true,
                false),
            new SimpleColumn(
                expectedTableName,
                "_PARTTIONDATE",
                TypeFactory.createSimpleType(TypeKind.TYPE_DATE),
                true,
                false),
            new SimpleColumn(
                expectedTableName,
                "_TABLE_SUFFIX",
                TypeFactory.createSimpleType(TypeKind.TYPE_STRING),
                true,
                false))));

  }
}
