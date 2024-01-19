package examples;

import com.borjav.data.model.ResolvedNodeExtended;
import com.borjav.data.output.OutputLineage;
import com.borjav.data.parser.ZetaSQLResolver;
import com.borjav.data.service.BigQueryServiceFactory;
import com.borjav.data.service.BigQueryTableLoadService;
import com.borjav.data.service.BigQueryZetaSqlSchemaLoader;
import org.junit.Test;
import utils.TestResourceLoader;

public class BigQuerySqlParserBQSchemaTest {


  @Test
  public void testEndToEnd() {
    // The default factory will try to connect to the real BigQuery API, which requires
    // the user to be authenticated:
    // $ gcloud auth application-default login
    BigQueryServiceFactory service = BigQueryServiceFactory.defaultFactory();

    BigQueryZetaSqlSchemaLoader schemaLoader =
        new BigQueryZetaSqlSchemaLoader(
            BigQueryTableLoadService.usingServiceFactory(service));
    ZetaSQLResolver parser = new ZetaSQLResolver(schemaLoader);

    String sql = TestResourceLoader.load("sql/benchmark/external/test.sql");
//    String sql = """
//        SELECT
//              word,
//              SUM(word_count) AS count
//            FROM
//              `bigquery-public-data.samples.shakespeare`
//            WHERE
//              word LIKE "%raisin%"
//            GROUP BY
//              word;
//        """;
    ResolvedNodeExtended table = parser.extractLineage(sql);
    OutputLineage printer = new OutputLineage();
    System.out.println(printer.toYaml(table, "", true));
  }
}
