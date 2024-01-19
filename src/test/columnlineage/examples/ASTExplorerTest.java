package examples;

import com.borjav.data.model.ResolvedNodeExtended;
import com.borjav.data.output.OutputLineage;
import com.borjav.data.parser.ZetaSQLResolver;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.zetasql.SimpleCatalog;
import java.io.File;
import org.junit.Assert;
import org.junit.Test;
import utils.FakeCatalogBuilder;
import utils.TestCase;

public class ASTExplorerTest {


  @Test
  public void testUnnestCreateView() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/unnest_create_view.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testUnnestCreate() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/unnest_create.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testUnnest() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/unnest.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testUdf() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/udf.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testTimestamps() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/timestamps.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testStgPayments() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/stg_payments.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testStgCustomers() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/stg_customers.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testSelectPrune() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/select_prune.yaml");
    genericTest(file, null, false);
  }


  @Test
  public void testSubqueryUnnest() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/subquery_unnest.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testPivot() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/pivot.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testMultipleRefs() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/multiple_refs.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testMessyStruct() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/messy_struct.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testCustomers() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/customers.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testJsonFunctions() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/json_functions.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testGroupBy() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/customers_groupby.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testGroupBySets() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/customers_groupby_sets.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testJsonFunctionsStruct() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/json_functions_struct.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testParameters() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/parameter.yaml");
    genericTest(file, null, false);
  }


  @Test
  public void testJsonWithLiterals() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/json_functions_with_literals.yaml");
    genericTest(file, null, true);
  }

  @Test
  public void testMessyUnnesting() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/messy_unnesting.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testAnalyticalFunctions() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/analytical_functions.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testPivotWithLiterals() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/pivot_where_with_literals.yaml");
    genericTest(file, null, true);
  }


  @Test
  public void testRecursiveCTE() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/recursive_cte.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testLookerSubqueries() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/looker_subquery.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testLookerSubqueryCrazy() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/looker_subquery_crazy.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testCount() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/count.yaml");
    genericTest(file, null, false);
  }

  @Test
  public void testArrayAggMultiNests() throws Exception {
    File file = new File("src/test/resources/sql/benchmark/array_agg_multiplenests.yaml");
    genericTest(file, null, false);
  }

  public static void genericTest(File file, SimpleCatalog catalog, boolean printLeafs)
      throws Exception {
    if (catalog == null) {
      catalog = FakeCatalogBuilder.buildCatalog();
    }
    ZetaSQLResolver parser = new ZetaSQLResolver(catalog);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    OutputLineage printer = new OutputLineage();

    TestCase test = mapper.readValue(file, TestCase.class);
    String sql = parser.replaceQuotesFullyQualifiedName(test.query);
    ResolvedNodeExtended table = parser.extractLineage(sql);

    Assert.assertEquals(
        printer.toYaml(printer.toModel(table, test.expected_output.name, null, printLeafs)),
        printer.toYaml(test.expected_output));
  }
}
