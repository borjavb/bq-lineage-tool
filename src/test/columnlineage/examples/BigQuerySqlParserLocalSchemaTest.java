package examples;

import com.borjav.data.model.ResolvedNodeExtended;
import com.borjav.data.output.OutputLineage;
import com.borjav.data.parser.ZetaSQLResolver;
import com.borjav.data.service.BigQueryTableLoadService;
import com.borjav.data.service.BigQueryZetaSqlSchemaLoader;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Assert;
import org.junit.Test;
import utils.FakeBigQueryServiceFactory;
import utils.TestCase;
import utils.TestResourceLoader;

public final class BigQuerySqlParserLocalSchemaTest {

  @Test
  public void extractColumnLineage_concatColumns_correctColumnNames()
      throws JsonProcessingException {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory
            .forTableSchemas(
                TestResourceLoader.load("schemas/tableA_schema.json"),
                TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ZetaSQLResolver parser = new ZetaSQLResolver(fakeSchemaLoader);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    OutputLineage printer = new OutputLineage();

    String inputTest = TestResourceLoader.load("sql/kitchen_sink_concat.yaml");
    TestCase testString = mapper.readValue(inputTest, TestCase.class);
    String sql = parser.replaceQuotesFullyQualifiedName(testString.query);
    ResolvedNodeExtended table = parser.extractLineage(sql);

    Assert.assertEquals(
        printer.toYaml(printer.toModel(table, testString.expected_output.name, null,false)),
        printer.toYaml(testString.expected_output));

  }

  @Test
  public void extractColumnLineage_multipleOutputColumnsWithAlias_correctColumnLineage()
      throws JsonProcessingException {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/tableA_schema.json"),
            TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ZetaSQLResolver parser = new ZetaSQLResolver(fakeSchemaLoader);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    OutputLineage printer = new OutputLineage();

    String inputTest = TestResourceLoader.load("sql/kitchen_sink_multiple_output_columns_with_alias.yaml");
    TestCase testString = mapper.readValue(inputTest, TestCase.class);
    String sql = parser.replaceQuotesFullyQualifiedName(testString.query);
    ResolvedNodeExtended table = parser.extractLineage(sql);

    Assert.assertEquals(
        printer.toYaml(printer.toModel(table, testString.expected_output.name, null,false)),
        printer.toYaml(testString.expected_output));
  }

  @Test
  public void extractColumnLineage_multipleOutputColumnsWithoutAlias_correctColumnLineage()
      throws JsonProcessingException {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/tableA_schema.json"),
            TestResourceLoader.load("schemas/tableB_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ZetaSQLResolver parser = new ZetaSQLResolver(fakeSchemaLoader);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    OutputLineage printer = new OutputLineage();

    String inputTest = TestResourceLoader.load("sql/kitchen_sink_multiple_output_columns_without_alias.yaml");
    TestCase testString = mapper.readValue(inputTest, TestCase.class);
    String sql = parser.replaceQuotesFullyQualifiedName(testString.query);
    ResolvedNodeExtended table = parser.extractLineage(sql);

    Assert.assertEquals(
        printer.toYaml(printer.toModel(table, testString.expected_output.name, null,false)),
        printer.toYaml(testString.expected_output));
  }

  @Test
  public void
  extractColumnLineage_bigQuerySchemaMultipleOutputColumnsWithoutAlias_correctColumnLineage()
      throws JsonProcessingException {
    FakeBigQueryServiceFactory fakeBigqueryFactory =
        FakeBigQueryServiceFactory.forTableSchemas(
            TestResourceLoader.load("schemas/daily_report_table_schema.json"),
            TestResourceLoader.load("schemas/error_stats_table_schema.json"));
    BigQueryZetaSqlSchemaLoader fakeSchemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(fakeBigqueryFactory));

    ZetaSQLResolver parser = new ZetaSQLResolver(fakeSchemaLoader);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    OutputLineage printer = new OutputLineage();

    String inputTest = TestResourceLoader.load(
        "sql/bigquery_daily_report_error_stats_join_group_by_aggr_functions.yaml");
    TestCase testString = mapper.readValue(inputTest, TestCase.class);
    String sql = parser.replaceQuotesFullyQualifiedName(testString.query);
    ResolvedNodeExtended table = parser.extractLineage(sql);

    Assert.assertEquals(
        printer.toYaml(printer.toModel(table, testString.expected_output.name, null,false)),
        printer.toYaml(testString.expected_output));


  }
}
