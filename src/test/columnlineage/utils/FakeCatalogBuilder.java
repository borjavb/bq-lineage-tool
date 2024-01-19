package utils;

import com.google.common.collect.ImmutableList;
import com.google.zetasql.SimpleCatalog;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.StructType;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType;

public class FakeCatalogBuilder {


  public static SimpleCatalog buildCatalog() {

    SimpleCatalog datasetCatalog = new SimpleCatalog("jaffle_shop");

    SimpleTable rawCustomers = new SimpleTable("catalog.jaffle_shop.raw_customers");
    rawCustomers.addSimpleColumn("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    rawCustomers.addSimpleColumn("first_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    rawCustomers.addSimpleColumn("last_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(rawCustomers);

    SimpleTable rawOrders = new SimpleTable("catalog.jaffle_shop.raw_orders");
    rawOrders.addSimpleColumn("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    rawOrders.addSimpleColumn("user_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    rawOrders.addSimpleColumn("order_date",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_DATE));
    rawOrders.addSimpleColumn("status",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(rawOrders);

    SimpleTable rawPayments = new SimpleTable("catalog.jaffle_shop.raw_payments");
    rawPayments.addSimpleColumn("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    rawPayments.addSimpleColumn("order_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    rawPayments.addSimpleColumn("payment_method",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    rawPayments.addSimpleColumn("amount",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    datasetCatalog.addSimpleTable(rawPayments);

    SimpleTable stgPayments = new SimpleTable("catalog.jaffle_shop.stg_payments");
    stgPayments.addSimpleColumn("payment_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    stgPayments.addSimpleColumn("order_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    stgPayments.addSimpleColumn("payment_method",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgPayments.addSimpleColumn("amount",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_FLOAT));
    datasetCatalog.addSimpleTable(stgPayments);

    SimpleTable stgOrders = new SimpleTable("catalog.jaffle_shop.stg_orders");
    stgOrders.addSimpleColumn("order_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    stgOrders.addSimpleColumn("customer_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    stgOrders.addSimpleColumn("order_date",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_DATE));
    stgOrders.addSimpleColumn("status",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(stgOrders);

    SimpleTable stgCustomers = new SimpleTable("catalog.jaffle_shop.stg_customers");
    stgCustomers.addSimpleColumn("customer_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_INT64));
    stgCustomers.addSimpleColumn("first_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgCustomers.addSimpleColumn("last_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(stgCustomers);

    SimpleTable stgStructTable = new SimpleTable("catalog.jaffle_shop.struct_table");
    StructType.StructField id = new StructType.StructField("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField other_id = new StructType.StructField("other_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));

    StructType.StructField sub_b = new StructType.StructField("sub_b",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField sub_a = new StructType.StructField("sub_a",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField sub_c = new StructType.StructField("sub_c",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_BIGNUMERIC));
    StructType.StructField subnested_id = new StructType.StructField("subnested_id",
        TypeFactory.createStructType(ImmutableList.of(sub_b, sub_c, sub_a)));
    stgStructTable.addSimpleColumn("content",
        TypeFactory.createStructType(ImmutableList.of(id, other_id, subnested_id)));
    stgStructTable.addSimpleColumn("first_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgStructTable.addSimpleColumn("last_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(stgStructTable);

    SimpleTable stgNestedTable = new SimpleTable("catalog.jaffle_shop.nested_table");
    StructType.StructField nested_id = new StructType.StructField("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField nested_other_id = new StructType.StructField("other_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField nested_amount = new StructType.StructField("amount",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_BIGNUMERIC));
    stgNestedTable.addSimpleColumn("nested_ids",
        TypeFactory.createArrayType(TypeFactory.createStructType(ImmutableList.of(nested_id,
            nested_other_id, nested_amount))));
    StructType.StructField nested_id2 = new StructType.StructField("key",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField nested_other_id2 = new StructType.StructField("value",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField nested_amount2 = new StructType.StructField("otherthing",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_BIGNUMERIC));
    stgNestedTable.addSimpleColumn("labels",
        TypeFactory.createArrayType(TypeFactory.createStructType(ImmutableList.of(nested_id2,
            nested_other_id2, nested_amount2))));
    stgNestedTable.addSimpleColumn("first_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgNestedTable.addSimpleColumn("last_name",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgNestedTable.addSimpleColumn("_PARTITIONTIME",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP));
    stgNestedTable.addSimpleColumn("anotherdate",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_TIMESTAMP));
    stgNestedTable.addSimpleColumn("cost",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_BIGNUMERIC));
    StructType.StructField anothernesting = new StructType.StructField("anothernesting",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField anothernesting2 = new StructType.StructField("anothernesting2",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField anothernesting23 = new StructType.StructField("anothernesting23",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgNestedTable.addSimpleColumn("rescordenesed",
        TypeFactory.createStructType(
            ImmutableList.of(anothernesting, anothernesting2, anothernesting23)));
    StructType.StructField thisisa = new StructType.StructField("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField valueanother = new StructType.StructField("yetmvalue",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    stgNestedTable.addSimpleColumn("acolumn",
        TypeFactory.createStructType(ImmutableList.of(thisisa, valueanother)));
    stgNestedTable.addSimpleColumn("_TABLE_SUFFIX",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));

    datasetCatalog.addSimpleTable(stgNestedTable);

    SimpleTable jsonTable = new SimpleTable("catalog.jaffle_shop.json_table");
    jsonTable.addSimpleColumn("json_field",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_JSON));
    jsonTable.addSimpleColumn("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(jsonTable);

    SimpleTable timestampTable = new SimpleTable("catalog.jaffle_shop.timestamps");
    timestampTable.addSimpleColumn("_TABLE_SUFFIX",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    timestampTable.addSimpleColumn("id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    timestampTable.addSimpleColumn("other_id",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    timestampTable.addSimpleColumn("source",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    timestampTable.addSimpleColumn("value",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_FLOAT));
    datasetCatalog.addSimpleTable(timestampTable);

    SimpleTable doubleNested = new SimpleTable("catalog.jaffle_shop.multiplenests");
    StructType.StructField a = new StructType.StructField("a",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField b = new StructType.StructField("b",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField c = new StructType.StructField("c",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField firstnesting = new StructType.StructField("firstnesting",
        TypeFactory.createStructType(ImmutableList.of(a, b, c)));
    StructType.StructField d = new StructType.StructField("d",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    StructType.StructField secondnesting = new StructType.StructField("secondnesting",
        TypeFactory.createStructType(ImmutableList.of(firstnesting, d)));
    doubleNested.addSimpleColumn("triplenest",
        TypeFactory.createStructType(ImmutableList.of(secondnesting)));
    doubleNested.addSimpleColumn("_TABLE_SUFFIX",
        TypeFactory.createSimpleType(ZetaSQLType.TypeKind.TYPE_STRING));
    datasetCatalog.addSimpleTable(doubleNested);

    return datasetCatalog;

  }
}
